package main

import (
	"context"
	"net/http"
	"os"
	"strconv"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"
	"github.com/zincsearch/zincsearch/pkg/cluster"
)

func SetupHttp(r *gin.Engine) {
	// set release as default gin mode.
	if mode := os.Getenv(gin.EnvGinMode); mode == "" {
		gin.SetMode(gin.ReleaseMode)
	}
	r.Use(gin.Recovery())

	r.Use(cors.New(cors.Config{
		AllowOrigins:     []string{"*"},
		AllowMethods:     []string{"GET", "POST", "DELETE", "PUT", "HEAD", "OPTIONS"},
		AllowHeaders:     []string{"Origin", "authorization", "content-type"},
		ExposeHeaders:    []string{"Content-Length"},
		AllowCredentials: true,
	}))

	r.PUT("/api/cluster-nodes", PutClusterInfo)
	r.POST("/api/cluster-nodes", SetNodeInfo)
	r.GET("/api/cluster-nodes", GetNodeInfos)
	r.DELETE("/api/cluster-nodes", RemoveNode)
}

func PutClusterInfo(c *gin.Context) {
	var clusterInfo []cluster.NodeInfo
	err := c.BindJSON(&clusterInfo)
	if err != nil {
		c.Status(http.StatusBadRequest)
		return
	}

	if err = cluster.PutClusterInfo(context.Background(), clusterInfo); err != nil {
		c.String(http.StatusInternalServerError, "put cluster info error: %s", err)
	}
	c.String(http.StatusOK, "success")
}

func RemoveNode(c *gin.Context) {
	nodeId := c.Query("id")
	if nodeId == "" {
		c.String(http.StatusBadRequest, "require id")
		return
	}
	nodes, err := cluster.GetClusterInfo(context.Background())
	if err != nil {
		log.Errorf("get cluster node info error: %s", err)
		c.Status(http.StatusInternalServerError)
		return
	}
	newNodes := make([]cluster.NodeInfo, 0)
	for _, node := range nodes {
		if strconv.Itoa(node.NodeId) == nodeId {
			continue
		}
		newNodes = append(newNodes, node)
	}

	err = cluster.PutClusterInfo(context.Background(), newNodes)
	if err != nil {
		log.Errorf("put cluster node info error: %s", err)
		c.Status(http.StatusInternalServerError)
		return
	}
	c.String(http.StatusOK, "success")
}

func SetNodeInfo(c *gin.Context) {
	var nodeInfo cluster.NodeInfo
	err := c.BindJSON(&nodeInfo)
	if err != nil {
		c.Status(http.StatusBadRequest)
		return
	}

	nodes, err := cluster.GetClusterInfo(context.Background())
	if err != nil {
		log.Errorf("get cluster node info error: %s", err)
		c.Status(http.StatusInternalServerError)
		return
	}

	update := false
	for i, node := range nodes {
		if node.NodeId == nodeInfo.NodeId {
			nodes[i] = nodeInfo
			update = true
			break
		}
	}
	if !update {
		nodes = append(nodes, nodeInfo)
	}
	err = cluster.PutClusterInfo(context.Background(), nodes)
	if err != nil {
		log.Errorf("put cluster node info error: %s", err)
		c.Status(http.StatusInternalServerError)
		return
	}
	c.String(http.StatusOK, "success")
}

type NodeInfoResp struct {
	NodeId  int    `json:"node_id"`
	Address string `json:"address"`
	Alive   bool   `json:"alive"`
}

func GetNodeInfos(c *gin.Context) {
	nodes, err := cluster.GetClusterInfo(context.Background())
	if err != nil {
		log.Errorf("get cluster node info error: %s", err)
		c.Status(http.StatusInternalServerError)
		return
	}

	aliveNodes, err := cluster.ListAvailableNodes(context.Background())
	if err != nil {
		log.Errorf("get alive nodes error: %s", err)
		c.Status(http.StatusInternalServerError)
		return
	}
	aliveMap := make(map[int]struct{})
	for _, aliveId := range aliveNodes {
		aliveMap[aliveId] = struct{}{}
	}

	result := make([]NodeInfoResp, len(nodes))

	for i, node := range nodes {
		_, alive := aliveMap[node.NodeId]
		resp := NodeInfoResp{
			NodeId:  node.NodeId,
			Address: node.Address,
			Alive:   alive,
		}
		result[i] = resp
	}

	c.JSON(http.StatusOK, result)
}
