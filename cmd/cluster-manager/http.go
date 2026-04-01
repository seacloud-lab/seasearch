package main

import (
	"fmt"
	"net/http"
	"net/url"
	"os"
	"slices"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/haiwen/goutils/clusterkit"
	log "github.com/sirupsen/logrus"
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

	r.GET("/api/cluster/nodes", listClusterNodes)
	r.POST("/api/cluster/nodes", createClusterNodes)
	r.PUT("/api/cluster/nodes", updateClusterNodes)
	r.DELETE("/api/cluster/nodes", deleleClusterNodes)
	r.POST("/api/cluster/clean", cleanClusterData)
}

type ClusterNode struct {
	ID  int    `json:"id"`
	URL string `json:"url"`
}

type ListClusterNodesResponse struct {
	NodeId int    `json:"node_id"`
	URL    string `json:"url"`
	Alive  bool   `json:"alive"`
}

func listClusterNodes(c *gin.Context) {
	cluster, _, err := clusterkit.GetClusterNodes(c.Request.Context())
	if err != nil {
		log.Errorf("failed to get cluster nodes: %v", err)
		c.Status(http.StatusInternalServerError)
		return
	}

	alive := make(map[int]bool)
	beats, err := clusterkit.ListHeartbeats(c.Request.Context())
	for _, beat := range beats {
		alive[beat.NodeID] = true
	}

	var rsp []ListClusterNodesResponse
	for _, node := range cluster.Nodes {
		rsp = append(rsp, ListClusterNodesResponse{
			NodeId: node.ID,
			URL:    node.URL,
			Alive:  alive[node.ID],
		})
	}

	c.JSON(http.StatusOK, rsp)
}

type CreateClusterNodesRequest struct {
	Nodes []ClusterNode `json:"nodes"`
}

func createClusterNodes(c *gin.Context) {
	var req CreateClusterNodesRequest
	err := c.BindJSON(&req)
	if err != nil {
		c.Status(http.StatusBadRequest)
		return
	}

	cluster, _, err := clusterkit.GetClusterNodes(c.Request.Context())
	if err != nil {
		log.Errorf("failed to get cluster nodes %v", err)
		c.Status(http.StatusInternalServerError)
		return
	}
	var ids []int
	for _, node := range cluster.Nodes {
		ids = append(ids, node.ID)
	}

	for _, node := range req.Nodes {
		if node.ID == 0 {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "node id must be non-zero",
			})
			return
		}
		if slices.Contains(ids, node.ID) {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": fmt.Sprintf("node id %v already exist", node.ID),
			})
			return
		}
		if node.URL == "" {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "node url must be specified",
			})
			return
		}
		if _, err := url.Parse(node.URL); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": fmt.Sprintf("failed to parse node url: %v", err),
			})
			return
		}

		cluster.Nodes = append(cluster.Nodes, clusterkit.ClusterNode{
			ID:  node.ID,
			URL: node.URL,
		})
	}

	err = clusterkit.SetClusterNode(cluster)
	if err != nil {
		log.Errorf("failed to set cluster node: %v", err)
		c.Status(http.StatusInternalServerError)
		return
	}
}

type UpdateClusterNodesRequest struct {
	Nodes []ClusterNode `json:"nodes"`
}

func updateClusterNodes(c *gin.Context) {
	var req UpdateClusterNodesRequest
	err := c.BindJSON(&req)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
		})
		return
	}

	cluster, _, err := clusterkit.GetClusterNodes(c.Request.Context())
	if err != nil {
		log.Errorf("failed to get cluster nodes: %v", err)
		c.Status(http.StatusInternalServerError)
		return
	}

	for _, node := range req.Nodes {
		if node.ID == 0 {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "node id must be non-zero",
			})
			return
		}
		i := slices.IndexFunc(cluster.Nodes,
			func(n clusterkit.ClusterNode) bool { return n.ID == node.ID },
		)
		if i < 0 {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": fmt.Sprintf("node id %v not found", node.ID),
			})
			return
		}
		if node.URL == "" {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "node addr must be specified",
			})
			return
		}
		if _, err := url.Parse(node.URL); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": fmt.Sprintf("failed to parse node url: %v", err),
			})
			return
		}

		cluster.Nodes[i].URL = node.URL
	}

	err = clusterkit.SetClusterNode(cluster)
	if err != nil {
		log.Errorf("failed to set cluster node: %v", err)
		c.Status(http.StatusInternalServerError)
		return
	}
}

type DeleteClusterNodesRequest struct {
	NodeIDs []int `json:"node_ids"`
}

func deleleClusterNodes(c *gin.Context) {
	var req DeleteClusterNodesRequest
	err := c.BindJSON(&req)
	if err != nil {
		c.Status(http.StatusBadRequest)
		return
	}

	cluster, _, err := clusterkit.GetClusterNodes(c.Request.Context())
	if err != nil {
		log.Errorf("failed to get cluster nodes: %v", err)
		c.Status(http.StatusInternalServerError)
		return
	}

	for _, id := range req.NodeIDs {
		cluster.Nodes = slices.DeleteFunc(cluster.Nodes,
			func(node clusterkit.ClusterNode) bool { return node.ID == id },
		)
	}

	err = clusterkit.SetClusterNode(cluster)
	if err != nil {
		log.Errorf("failed to set cluster node: %v", err)
		c.Status(http.StatusInternalServerError)
		return
	}
}

func cleanClusterData(c *gin.Context) {
	err := clusterkit.Delete(c.Request.Context(), "/cluster/nodes", false)
	if err != nil {
		log.Errorf("failed to delete etcd key: %v", err)
		c.Status(http.StatusInternalServerError)
		return
	}
	err = clusterkit.Delete(c.Request.Context(), "/cluster/assign/", true)
	if err != nil {
		log.Errorf("failed to delete etcd key: %v", err)
		c.Status(http.StatusInternalServerError)
		return
	}
}
