package hashring

import (
	"strconv"
	"testing"
)

func TestNewCluster(t *testing.T) {
	cluster := NewHashRingCluster(100)
	if cluster.virtualNodes == nil {
		t.Error("Virtual Nodes not allocated")
	}

	if len(cluster.virtualNodes) != 100 {
		t.Errorf("Virtual nodes not allocated. %d should be 100", len(cluster.virtualNodes))
	}

	if cluster.numberOfVirtualNodes != 100 {
		t.Errorf("Number of virtual nodes incorrect %d should be 100", cluster.numberOfVirtualNodes)
	}

	if cluster.servers == nil || cluster.virtualToServerMapping == nil {
		t.Error("servers array and virtualToServerMapping uninitialized")
	}
}

func TestSimpleAddServer(t *testing.T) {
	cluster := NewHashRingCluster(100)
	err := cluster.AddServer("server1", "0-99")
	if err != nil {
		t.Errorf("Failed to add Server %v", err)
	}

	if len(cluster.servers) == 0 {
		t.Errorf("Failed to add serverInfo")
	}

	server := cluster.servers[0]
	if server.idx != 0 {
		t.Errorf("Index is incorrect %d, should be 0", server.idx)
	}

	if server.name != "server1" {
		t.Errorf("server Name not properly setup: %s expected 'server1'", server.name)
	}

	m := map[int]bool{}
	found := true

	for _, v := range server.virtualNodes {
		m[v.idx] = true
	}

	for i := 0; i < 100; i++ {
		if _, ok := m[i]; !ok {
			t.Errorf("Failed to find index %d in virtualNodes", i)
			found = false
		}
	}

	for i := 0; i < 100; i++ {
		istr := strconv.Itoa(i)
		if serverInfo, ok := cluster.virtualToServerMapping[istr]; !ok {
			t.Errorf("Failed to update VirtualToServerMapping")
		} else {
			if serverInfo.name != "server1" {
				t.Error("Incorrect server name")
			}
		}
	}

	if !found {
		t.Error("Issue in allocating virtualNodes")
	}

	for i := 0; i < 100; i++ {
		if cluster.virtualNodes[i].serverInfo == nil {
			t.Errorf("Failed to setup serverInfo")
		}

		if cluster.virtualNodes[i].serverInfo != cluster.servers[0] {
			t.Errorf("Virtual node %d not pointing to correct server", i)
		}
	}
}

func TestAddServerWithOverlap(t *testing.T) {
	cluster := NewHashRingCluster(100)
	cluster.AddServer("server1", "0-99")
	cluster.AddServer("server2", "50-99")
	server1Info := cluster.GetServerInfo("server1")
	server2Info := cluster.GetServerInfo("server2")

	if len(server1Info.virtualNodes) != 50 {
		t.Errorf("Server1 doesnt have correct number of virtualNodes %d->%d", len(server1Info.virtualNodes), 50)
	}

	if len(server2Info.virtualNodes) != 50 {
		t.Errorf("Server2 doesnt have correct number of virtualNodes %d->%d", len(server2Info.virtualNodes), 50)
	}

	for _, vnode := range server1Info.virtualNodes {
		if vnode.serverInfo != server1Info {
			t.Errorf("Virtual node not setup properly server1 -> %s", vnode.serverInfo.name)
		}
	}

	for _, vnode := range server2Info.virtualNodes {
		if vnode.serverInfo != server2Info {
			t.Errorf("Virtual node not setup properly server2 -> %s", vnode.serverInfo.name)
		}
	}

}

func TestAddServerInvalidRange(t *testing.T) {
	cluster := NewHashRingCluster(100)
	if cluster.AddServer("server1", "99-0") == nil {
		t.Errorf("Incorrect checking for swapped indices")
	}

	if cluster.AddServer("server1", "0-1000") == nil {
		t.Errorf("Incorrect our of bounds checking")
	}

	if cluster.AddServer("server1", "100-100") == nil {
		t.Error("Not checking for edge cases on start")
	}

	if cluster.AddServer("server1", "0-100") == nil {
		t.Error("Not checking for edge cases on end")
	}
}

func TestParseRange(t *testing.T) {
	start, end, err := parseRange(" 1 - 100 ")
	if start != 1 || end != 100 || err != nil {
		t.Errorf("Failed to parse %s properly %d, %d, %v expected 1, 100, nil", " 1 - 100 ", start, end, err)
	}

	start, end, err = parseRange("1-100")
	if start != 1 || end != 100 || err != nil {
		t.Errorf("Failed to parse %s properly %d, %d, %v expected 1, 100, nil", "1-100", start, end, err)
	}

}

func TestGetServer(t *testing.T) {
	cluster := NewHashRingCluster(100)
	cluster.AddServer("server1", "0-49")
	cluster.AddServer("server2", "49-99")

	found := []string{}
	for i := 0; i < 100; i++ {
		found = append(found, cluster.GetServer(strconv.Itoa(i)))
	}

	for i := 0; i < 100; i++ {
		server := cluster.GetServer(strconv.Itoa(i))
		if found[i] != server {
			t.Errorf("Inconsistent hash %d %s -> %s", i, server, found[i])
		}
	}
}

func TestSplit(t *testing.T) {
	cluster := NewHashRingCluster(150)
	cluster.AddServer("server1", "0-49")
	cluster.AddServer("server2", "50-99")
	cluster.AddServer("server3", "100-149")

	found := []string{}
	for i := 0; i < 1000; i++ {
		server := cluster.GetServer(strconv.Itoa(i))
		found = append(found, server)
	}

	cluster.Split("server1", "server1a")

	afterSplit := []string{}
	for i := 0; i < 1000; i++ {
		server := cluster.GetServer(strconv.Itoa(i))
		afterSplit = append(afterSplit, server)
	}

	for i := 0; i < 1000; i++ {
		if found[i] != afterSplit[i] {
			if found[i] != "server1" {
				t.Errorf("Hitting other nodes %s -> %s", found[i], afterSplit[i])
			} else {
				if afterSplit[i] != "server1a" {
					t.Errorf("Shifted to wrong server %s", afterSplit[i])
				}
			}
		}
	}

	server1aInfo := cluster.GetServerInfo("server1a")

	if len(server1aInfo.virtualNodes) != 25 {
		t.Errorf("Incorrect Split of server1a %d -> %d", len(server1aInfo.virtualNodes), 25)
	}

	for _, vn := range server1aInfo.virtualNodes {
		if vn.serverInfo != server1aInfo {
			t.Errorf("virtual node has wrong server for server1a")
		}
	}

	server1Info := cluster.GetServerInfo("server1")

	if len(server1Info.virtualNodes) != 25 {
		t.Errorf("Incorrect Split of server1 %d -> %d", len(server1Info.virtualNodes), 25)
	}
	for _, vn := range server1Info.virtualNodes {
		if vn.serverInfo != server1Info {
			t.Errorf("virtual node has wrong server for server4")
		}
	}

	for i := 0; i < 1000; i++ {
		t.Logf("%d: %s->%s", i, found[i], afterSplit[i])
	}
}
