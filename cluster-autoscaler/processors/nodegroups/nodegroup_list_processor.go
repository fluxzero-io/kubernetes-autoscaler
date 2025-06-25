/*
Copyright 2018 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package nodegroups

import (
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	ca_context "k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/utils/labels"
)

// NodeGroupListProcessor processes lists of NodeGroups considered in scale-up.
type NodeGroupListProcessor interface {
	Process(autoscalingCtx *ca_context.AutoscalingContext, nodeGroups []cloudprovider.NodeGroup,
		nodeInfos map[string]*framework.NodeInfo,
		unschedulablePods []*apiv1.Pod) ([]cloudprovider.NodeGroup, map[string]*framework.NodeInfo, error)
	CleanUp()
}

// NoOpNodeGroupListProcessor is returning pod lists without processing them.
type NoOpNodeGroupListProcessor struct {
}

// NewDefaultNodeGroupListProcessor creates an instance of NodeGroupListProcessor.
func NewDefaultNodeGroupListProcessor() NodeGroupListProcessor {
	return &AutoprovisioningNodeGroupListProcessor{}
}

// Process processes lists of unschedulable and scheduled pods before scaling of the cluster.
func (p *NoOpNodeGroupListProcessor) Process(autoscalingCtx *ca_context.AutoscalingContext, nodeGroups []cloudprovider.NodeGroup, nodeInfos map[string]*framework.NodeInfo,
	unschedulablePods []*apiv1.Pod) ([]cloudprovider.NodeGroup, map[string]*framework.NodeInfo, error) {
	return nodeGroups, nodeInfos, nil
}

// CleanUp cleans up the processor's internal structures.
func (p *NoOpNodeGroupListProcessor) CleanUp() {
}

type AutoprovisioningNodeGroupListProcessor struct {
}

// Process processes lists of unschedulable and scheduled pods before scaling of the cluster.
// Process extends the list of node groups
func (p *AutoprovisioningNodeGroupListProcessor) Process(autoscalingCtx *ca_context.AutoscalingContext, nodeGroups []cloudprovider.NodeGroup, nodeInfos map[string]*framework.NodeInfo,
	unschedulablePods []*apiv1.Pod,
) ([]cloudprovider.NodeGroup, map[string]*framework.NodeInfo, error) {
	machines, err := autoscalingCtx.CloudProvider.GetAvailableMachineTypes()
	if err != nil {
		return nil, nil, err
	}
	bestLabels := labels.BestLabelSet(unschedulablePods)
	for _, machineType := range machines {
		nodeGroup, err := autoscalingCtx.CloudProvider.NewNodeGroup(machineType, bestLabels, map[string]string{}, []apiv1.Taint{}, map[string]resource.Quantity{})
		if err != nil {
			return nil, nil, err
		}
		nodeInfo, err := nodeGroup.TemplateNodeInfo()
		if err != nil {
			return nil, nil, err
		}
		nodeInfos[nodeGroup.Id()] = nodeInfo
		nodeGroups = append(nodeGroups, nodeGroup)
	}
	return nodeGroups, nodeInfos, nil
}

// CleanUp cleans up the processor's internal structures.
func (p *AutoprovisioningNodeGroupListProcessor) CleanUp() {
}
