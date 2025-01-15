package main

import (
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/zincsearch/zincsearch/pkg/handlers/auth"
	"github.com/zincsearch/zincsearch/pkg/handlers/index"
	"github.com/zincsearch/zincsearch/pkg/meta"
	"github.com/zincsearch/zincsearch/pkg/routes"
	"github.com/zincsearch/zincsearch/pkg/zutils"
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

	r.Use(func(c *gin.Context) {
		start := time.Now()
		c.Next()
		duration := time.Since(start).Seconds()
		accessLog.Info().Msgf("\"%s %s %s\" %d %f", c.Request.Method, c.Request.RequestURI, c.Request.Proto, c.Writer.Status(), duration)
	})

	// cluster manager
	r.PUT("/api/cluster-nodes", forwardToManager)
	r.POST("/api/cluster-nodes", forwardToManager)
	r.GET("/api/cluster-nodes", forwardToManager)
	r.DELETE("/api/cluster-nodes", forwardToManager)

	// auth
	r.POST("/api/login", auth.Login)
	r.POST("/api/user", AuthMiddlewareNoCache("auth.CreateUpdateUser"), auth.CreateUpdateUser)
	r.PUT("/api/user", AuthMiddlewareNoCache("auth.CreateUpdateUser"), auth.CreateUpdateUser)
	r.DELETE("/api/user/:id", AuthMiddlewareNoCache("auth.DeleteUser"), auth.DeleteUser)
	r.GET("/api/user", AuthMiddlewareNoCache("auth.ListUser"), auth.ListUser)
	r.GET("/api/permissions", AuthMiddlewareNoCache("auth.ListPermissions"), auth.ListPermissions)
	r.GET("/api/role", AuthMiddlewareNoCache("auth.ListRole"), auth.ListRole)
	r.POST("/api/role", AuthMiddlewareNoCache("auth.CreateUpdateRole"), auth.CreateUpdateRole)
	r.PUT("/api/role", AuthMiddlewareNoCache("auth.CreateUpdateRole"), auth.CreateUpdateRole)
	r.DELETE("/api/role/:id", AuthMiddlewareNoCache("auth.DeleteRole"), auth.DeleteRole)

	// index
	r.GET("/api/index", AuthMiddlewareNoCache("index.List"), List)
	r.GET("/api/index_name", AuthMiddlewareNoCache("index.IndexNameList"), IndexNameList)
	r.POST("/api/index", Create)
	r.PUT("/api/index", Create)
	r.PUT("/api/index/:target", Create)
	r.DELETE("/api/index/:target", Delete)
	r.GET("/api/index/:target", directForwarding)
	r.HEAD("/api/index/:target", AuthMiddlewareNoCache("index.Exists"), Exists)
	r.POST("/api/index/:target/refresh", directForwarding)

	// vector
	r.POST("/api/:target/_search/vector", SearchVector)
	r.POST("/api/:target/:field/_rebuild", directForwarding)
	r.POST("/api/:target/_recall", directForwarding)

	// index settings
	r.GET("/api/:target/_mapping", directForwarding)
	r.PUT("/api/:target/_mapping", directForwarding)
	r.GET("/api/:target/_settings", directForwarding)
	r.PUT("/api/:target/_settings", directForwarding)

	// analyze
	r.POST("/api/_analyze", AuthMiddlewareNoCache("index.Analyze"), index.Analyze) // without specified index, we can analyze directly
	r.POST("/api/:target/_analyze", directForwarding)

	// search
	r.POST("/api/:target/_search", directForwarding)

	// document
	// Document Bulk update/insert
	r.POST("/api/_bulk", Bulk)
	r.POST("/api/:target/_bulk", Bulk)
	r.POST("/api/:target/_multi", directForwarding)
	r.POST("/api/_bulkv2", BulkV2)         // New JSON format
	r.POST("/api/:target/_bulkv2", BulkV2) // New JSON format
	// Document CRUD APIs. Update is same as create.
	r.POST("/api/:target/_doc", directForwarding)        // create
	r.PUT("/api/:target/_doc", directForwarding)         // create
	r.PUT("/api/:target/_doc/:id", directForwarding)     // create or update
	r.HEAD("/api/:target/_doc/:id", directForwarding)    // get
	r.GET("/api/:target/_doc/:id", directForwarding)     // get
	r.POST("/api/:target/_update/:id", directForwarding) // update
	r.DELETE("/api/:target/_doc/:id", directForwarding)  // delete

	// es API
	r.POST("/es/_search", SearchDSL)
	r.POST("/es/_msearch", MultiSearch)
	r.POST("/es/:target/_search", SearchDSL)
	r.POST("/es/:target/_msearch", MultiSearch)
	r.POST("/es/:target/_delete_by_query", directForwarding)

	r.GET("/es/_index_template", AuthMiddlewareNoCache("index.ListTemplate"), index.ListTemplate)
	r.POST("/es/_index_template", AuthMiddlewareNoCache("index.CreateTemplate"), index.CreateTemplate)
	r.PUT("/es/_index_template/:target", AuthMiddlewareNoCache("index.CreateTemplate"), index.CreateTemplate)
	r.GET("/es/_index_template/:target", AuthMiddlewareNoCache("index.GetTemplate"), index.GetTemplate)
	r.HEAD("/es/_index_template/:target", AuthMiddlewareNoCache("index.GetTemplate"), index.GetTemplate)
	r.DELETE("/es/_index_template/:target", AuthMiddlewareNoCache("index.DeleteTemplate"), index.DeleteTemplate)

	r.PUT("/es/:target", Create)
	r.HEAD("/es/:target", Exists)

	r.GET("/es/:target/_mapping", directForwarding)
	r.PUT("/es/:target/_mapping", directForwarding)

	r.GET("/es/:target/_settings", directForwarding)
	r.PUT("/es/:target/_settings", directForwarding)

	r.POST("/es/_analyze", AuthMiddlewareNoCache("index.Analyze"), routes.ESMiddleware, index.Analyze)
	r.POST("/es/:target/_analyze", directForwarding)

	r.POST("/es/_bulk", EsBulk)
	r.POST("/es/:target/_bulk", EsBulk)
	r.PUT("/es/:target/_bulk", EsBulk)
	r.POST("/es/:target/_refresh", directForwarding)

	r.POST("/es/:target/_doc", directForwarding)        // create
	r.PUT("/es/:target/_doc/:id", directForwarding)     // create or update
	r.PUT("/es/:target/_create/:id", directForwarding)  // create
	r.POST("/es/:target/_create/:id", directForwarding) // create
	r.POST("/es/:target/_update/:id", directForwarding) // update part of document
	r.DELETE("/es/:target/_doc/:id", directForwarding)  // delete

}

// directForwarding
// if the index name is in Param, and we don't need to read body,
// we can handle request with this function
func directForwarding(c *gin.Context) {
	indexName := c.Param("target")
	addr, err := GetAddrByIndex(indexName)
	if err != nil {
		zutils.GinRenderJSON(c, http.StatusInternalServerError, meta.HTTPResponseError{Error: err.Error()})
		return
	}

	u := url.URL{Scheme: "http", Host: addr}
	proxyPool.Get(&u).ServeHTTP(c.Writer, c.Request)
}

func forwardToManager(c *gin.Context) {
	host := conf.Proxy.ClusterManagerAddr
	u := url.URL{Scheme: "http", Host: host}
	proxyPool.Get(&u).ServeHTTP(c.Writer, c.Request)
}
