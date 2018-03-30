package hashring

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

type ServerInfo struct {
	name         string
	idx          int
	virtualNodes []*VirtualNodeInfo
}

type VirtualNodeInfo struct {
	name       string
	idx        int
	serverInfo *ServerInfo
}

type HashRingCluster struct {
	ring                   *HashRing
	numberOfVirtualNodes   int
	servers                []*ServerInfo
	virtualToServerMapping map[string]*ServerInfo
	virtualNodes           []*VirtualNodeInfo
}

func NewHashRingCluster(numberOfVirtualNodes int) *HashRingCluster {
	cluster := &HashRingCluster{}
	cluster.servers = []*ServerInfo{}
	cluster.virtualToServerMapping = map[string]*ServerInfo{}
	cluster.virtualNodes = []*VirtualNodeInfo{}
	cluster.numberOfVirtualNodes = numberOfVirtualNodes

	nodeNames := []string{}
	for i := 0; i < numberOfVirtualNodes; i++ {
		name := strconv.Itoa(i)
		nodeNames = append(nodeNames, name)
		virtualNodeInfo := &VirtualNodeInfo{}
		virtualNodeInfo.name = name
		virtualNodeInfo.idx = i
		virtualNodeInfo.serverInfo = nil
		cluster.virtualNodes = append(cluster.virtualNodes, virtualNodeInfo)
	}

	cluster.ring = New(nodeNames)

	return cluster
}

func parseRange(r string) (start, end int, err error) {
	s := strings.Split(r, "-")
	if len(s) != 2 {
		err = errors.New("InvalidInput")
		return
	}

	start, err = strconv.Atoi(strings.TrimSpace(s[0]))
	if err != nil {
		return
	}

	end, err = strconv.Atoi(strings.TrimSpace(s[1]))
	return
}

func removeVirtualNode(vnodes []*VirtualNodeInfo, idx int) []*VirtualNodeInfo {
	i := 0
	for i = 0; i < len(vnodes); i++ {
		if vnodes[i].idx == idx {
			break
		}
	}
	return append(vnodes[:i], vnodes[i+1:]...)
}

/*AddServer
  rangeString: a-b where a and b are integers >= 0
*/
func (hc *HashRingCluster) AddServer(name string, rangeString string) error {
	start, end, err := parseRange(rangeString)
	if err != nil {
		return err
	}

	if start < 0 || start >= hc.numberOfVirtualNodes || start > end || end < 0 || end >= hc.numberOfVirtualNodes {
		return errors.New("INVALIDINPUT")
	}

	server := &ServerInfo{}

	server.name = name
	server.idx = len(hc.servers)
	hc.servers = append(hc.servers, server)

	for i := start; i <= end; i++ {
		if hc.virtualNodes[i].serverInfo != nil {
			oldServerInfo := hc.virtualNodes[i].serverInfo
			oldServerInfo.virtualNodes = removeVirtualNode(oldServerInfo.virtualNodes, i)
		}
		hc.virtualNodes[i].serverInfo = server

		server.virtualNodes = append(server.virtualNodes, hc.virtualNodes[i])
		hc.virtualToServerMapping[hc.virtualNodes[i].name] = server
	}
	return nil
}

func (hc *HashRingCluster) GetServer(key string) string {

	virtualNodeName, _ := hc.ring.GetNode(key)
	serverInfo, ok := hc.virtualToServerMapping[virtualNodeName]
	if !ok {
		return "BlackHole"
	}
	return serverInfo.name
}

func (hc *HashRingCluster) GetServerInfo(serverName string) *ServerInfo {
	for _, serverInfo := range hc.servers {
		if serverInfo.name == serverName {
			return serverInfo
		}
	}
	return nil
}

func (hc *HashRingCluster) Split(serverName string, newServerName string) error {

	serverInfo := hc.GetServerInfo(serverName)

	if serverInfo == nil {
		return errors.New("SERVERNOTFOUND")
	}
	numVNodes := len(serverInfo.virtualNodes)

	halfVNodes := int(numVNodes / 2)

	// AddServer is an overwrite with cleanup
	hc.AddServer(newServerName, fmt.Sprintf("%d-%d", halfVNodes, numVNodes-1))
	return nil
}
