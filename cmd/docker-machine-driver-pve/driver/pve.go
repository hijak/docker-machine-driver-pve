package driver

import (
	"context"
	"crypto/tls"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/luthermonson/go-proxmox"
)

const (
	// Polling interval for Proxmox task status.
	pveTaskPollingInterval = 3 * time.Second

	// Polling timeout for Proxmox task status.
	pveTaskPollingTimeout = 10 * time.Minute
)

// Creates a new Proxmox VE virtual machine from the current template.
// If TemplateMap is provided, selects a node using round-robin and uses the template for that node.
func (d *Driver) createPVEVirtualMachine(ctx context.Context) (int, error) {
	var targetNode string
	var template *proxmox.VirtualMachine
	var err error

	// If template map is provided, use multi-node distribution
	if len(d.TemplateMap) > 0 {
		// Select target node using round-robin
		targetNode, err = d.selectNodeForVM(ctx)
		if err != nil {
			return -1, fmt.Errorf("failed to select node for VM: %w", err)
		}

		// Get template for the selected node
		template, err = d.getPVETemplate(ctx, targetNode)
		if err != nil {
			return -1, fmt.Errorf("failed to get template for node '%s': %w", targetNode, err)
		}
	} else {
		// Use default behavior - single template
		template, err = d.getPVETemplate(ctx)
		if err != nil {
			return -1, err
		}
	}

	// Clone the template
	vmid, task, err := template.Clone(ctx, &proxmox.VirtualMachineCloneOptions{
		Name: d.MachineName,
		Pool: d.ResourcePoolName,
		Full: map[bool]uint8{false: 0, true: 1}[d.FullClone],
	})
	if err != nil {
		templateID := d.TemplateID
		if len(d.TemplateMap) > 0 && targetNode != "" {
			templateID = d.getTemplateForNode(targetNode)
		}
		return vmid, fmt.Errorf("failed to clone template ID='%d': %w", templateID, err)
	}

	if err := d.waitForPVETaskToSucceed(ctx, task); err != nil {
		templateID := d.TemplateID
		if len(d.TemplateMap) > 0 && targetNode != "" {
			templateID = d.getTemplateForNode(targetNode)
		}
		return vmid, fmt.Errorf("failed to clone template ID='%d': %w", templateID, err)
	}

	// If we selected a target node and the clone ended up on a different node, migrate it
	if targetNode != "" {
		// Get the newly created VM to check its current node
		newVM, err := d.getPVEVirtualMachine(ctx, vmid)
		if err != nil {
			return vmid, fmt.Errorf("failed to retrieve newly created VM ID='%d': %w", vmid, err)
		}

		// If the VM is on a different node than target, migrate it
		if newVM.Node != targetNode {
			migrateTask, err := newVM.Migrate(ctx, &proxmox.VirtualMachineMigrateOptions{
				Target: targetNode,
			})
			if err != nil {
				return vmid, fmt.Errorf("failed to migrate VM ID='%d' to node '%s': %w", vmid, targetNode, err)
			}

			if err := d.waitForPVETaskToSucceed(ctx, migrateTask); err != nil {
				return vmid, fmt.Errorf("failed to migrate VM ID='%d' to node '%s': %w", vmid, targetNode, err)
			}
		}
	}

	return vmid, nil
}

// Returns the template ID for a specific node.
// If TemplateMap is provided and contains the node, returns that template ID.
// Otherwise, returns the default TemplateID.
func (d *Driver) getTemplateForNode(nodeName string) int {
	if len(d.TemplateMap) > 0 {
		if templateID, exists := d.TemplateMap[nodeName]; exists {
			return templateID
		}
	}
	return d.TemplateID
}

// Returns the current Proxmox VE template.
// If nodeName is provided and TemplateMap is set, returns the template for that node.
func (d *Driver) getPVETemplate(ctx context.Context, nodeName ...string) (*proxmox.VirtualMachine, error) {
	var templateID int
	if len(nodeName) > 0 && nodeName[0] != "" {
		templateID = d.getTemplateForNode(nodeName[0])
	} else {
		templateID = d.TemplateID
	}

	template, err := d.getPVEVirtualMachine(ctx, templateID)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve Proxmox VE template ID='%d': %w", templateID, err)
	}

	return template, nil
}

// Returns a Proxmox VE virtual machine from the current resource pool.
func (d *Driver) getPVEVirtualMachine(ctx context.Context, vmid int) (*proxmox.VirtualMachine, error) {
	resourcePool, err := d.getCurrentPVEResourcePool(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve Proxmox VE virtual machine ID='%d': %w", vmid, err)
	}

	for _, member := range resourcePool.Members {
		if member.VMID > math.MaxInt {
			continue
		}

		if member.Type != "qemu" || int(member.VMID) != vmid {
			continue
		}

		return d.getPVEVirtualMachineOnNode(ctx, vmid, member.Node)
	}

	return nil, fmt.Errorf("failed to retrieve Proxmox VE virtual machine ID='%d' in resource pool name='%s': not found", vmid, d.ResourcePoolName)
}

// Returns Proxmox VE virtual machine from a given node.
func (d *Driver) getPVEVirtualMachineOnNode(ctx context.Context, vmid int, nodeName string) (*proxmox.VirtualMachine, error) {
	node, err := d.getPVEClient().Node(ctx, nodeName)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve Proxmox VE node name='%s': %w", nodeName, err)
	}

	vm, err := node.VirtualMachine(ctx, vmid)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve Proxmox VE virtual machine ID='%d' on node name='%s': %w", vmid, nodeName, err)
	}

	return vm, nil
}

// Returns the current Proxmox VE resource pool.
func (d *Driver) getCurrentPVEResourcePool(ctx context.Context) (*proxmox.Pool, error) {
	resourcePool, err := d.getPVEClient().Pool(ctx, d.ResourcePoolName)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve Proxmox VE resource pool name='%s': %w", d.ResourcePoolName, err)
	}

	return resourcePool, nil
}

// Blocks until a Proxmox VE task finishes successfully.
func (d *Driver) waitForPVETaskToSucceed(ctx context.Context, task *proxmox.Task) error {
	if err := task.Wait(ctx, pveTaskPollingInterval, pveTaskPollingTimeout); err != nil {
		return fmt.Errorf("failed waiting for task ID='%s' to complete: %w", task.ID, err)
	}

	if !task.IsSuccessful {
		return fmt.Errorf("task ID='%s' failed", task.ID)
	}

	return nil
}

// Returns a client for Proxmox VE.
func (d *Driver) getPVEClient() *proxmox.Client {
	if d.pveClient != nil {
		return d.pveClient
	}

	client := http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				//nolint:gosec
				InsecureSkipVerify: d.InsecureTLS,
			},
		},
	}

	pveURL, err := url.Parse(d.URL)
	if err != nil {
		// Note that parsing is already checked in SetConfigFromFlags()
		panic(fmt.Errorf("failed to parse Proxmox VE URL: %w", err).Error())
	}

	d.pveClient = proxmox.NewClient(
		pveURL.JoinPath("/api2/json").String(),
		proxmox.WithAPIToken(d.TokenID, d.TokenSecret),
		proxmox.WithHTTPClient(&client),
	)

	return d.pveClient
}

// Returns all available nodes from the resource pool.
func (d *Driver) getAvailableNodes(ctx context.Context) ([]string, error) {
	resourcePool, err := d.getCurrentPVEResourcePool(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve resource pool to get available nodes: %w", err)
	}

	nodeSet := make(map[string]bool)
	for _, member := range resourcePool.Members {
		if member.Type == "qemu" && member.Node != "" {
			nodeSet[member.Node] = true
		}
	}

	if len(nodeSet) == 0 {
		return nil, fmt.Errorf("no nodes found in resource pool '%s'", d.ResourcePoolName)
	}

	nodes := make([]string, 0, len(nodeSet))
	for node := range nodeSet {
		nodes = append(nodes, node)
	}

	sort.Strings(nodes)
	return nodes, nil
}

// Selects a node for VM placement using round-robin distribution.
// Counts existing VMs per node and selects the one with the fewest VMs.
func (d *Driver) selectNodeForVM(ctx context.Context) (string, error) {
	availableNodes, err := d.getAvailableNodes(ctx)
	if err != nil {
		return "", err
	}

	// If template map is provided, prefer nodes in the map but allow others as fallback
	// This allows users to configure specific nodes while still using default template for others
	if len(d.TemplateMap) > 0 {
		// Check if any nodes from the map are available
		hasMappedNodes := false
		for _, node := range availableNodes {
			if _, exists := d.TemplateMap[node]; exists {
				hasMappedNodes = true
				break
			}
		}
		if !hasMappedNodes {
			return "", fmt.Errorf("no nodes from template map are available in resource pool '%s'", d.ResourcePoolName)
		}
	}

	// Count existing VMs per node
	vmCounts := make(map[string]int)
	for _, node := range availableNodes {
		vmCounts[node] = 0
	}

	// Get all VMs in the resource pool and count those with docker-machine tag
	resourcePool, err := d.getCurrentPVEResourcePool(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to retrieve resource pool to count VMs: %w", err)
	}

	for _, member := range resourcePool.Members {
		if member.Type != "qemu" || member.Node == "" {
			continue
		}

		// Check if this VM has the docker-machine tag
		vm, err := d.getPVEVirtualMachineOnNode(ctx, int(member.VMID), member.Node)
		if err != nil {
			// Skip VMs we can't access
			continue
		}

		if vm.HasTag(pveMachineTag) {
			vmCounts[member.Node]++
		}
	}

	// Find node(s) with minimum VM count
	minCount := math.MaxInt
	for _, count := range vmCounts {
		if count < minCount {
			minCount = count
		}
	}

	// Select from nodes with minimum count (alphabetically first for determinism)
	candidates := make([]string, 0)
	for node, count := range vmCounts {
		if count == minCount {
			candidates = append(candidates, node)
		}
	}

	if len(candidates) == 0 {
		return "", fmt.Errorf("no suitable nodes found for VM placement")
	}

	sort.Strings(candidates)
	return candidates[0], nil
}
