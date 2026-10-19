package handler

import (
	"net/http"

	corev1 "github.com/cinvat/apimachinery/api/core/v1"
	"github.com/gin-gonic/gin"
)

// groupVersion is the version this document describes. It mirrors
// v1alpha1.APIVersion; the handler package cannot import its parent, so the
// string lives here and the route table ties the two together.
const groupVersion = "v1alpha1"

// APIResource and APIResourceList are the shared discovery contract from
// apimachinery, reused rather than redefined. APIResource is one kind: the
// join key a client uses to turn a manifest's kind into a collection path,
// plus the addressing facts (plural name, namespacing) it needs to build one.
type (
	APIResource     = corev1.APIResource
	APIResourceList = corev1.APIResourceList
)

// resources is the route table as data. It is the single place this version
// names its kinds, so discovery cannot drift from the routes below it: a new
// route means a new entry, and the verbs listed here are the ones actually
// mounted.
//
// The aggregate origin (kloudapi) folds this into /apis and the CLI resolves
// kind -> path from it, so a missing entry is a kind no client can find —
// just as a route without a handler is a verb nobody can call.
var resources = []APIResource{
	{Name: "nodes", SingularName: "node", Namespaced: false, Kind: "Node",
		Verbs: []string{"get", "list", "watch"}},
	{Name: "flavors", SingularName: "flavor", Namespaced: false, Kind: "Flavor",
		Verbs: []string{"get", "list", "create", "delete"}},
	{Name: "virtualmachines", SingularName: "virtualmachine", Namespaced: true, Kind: "VirtualMachine",
		Verbs: []string{"get", "list", "watch", "create", "patch", "delete"}},
	{Name: "volumes", SingularName: "volume", Namespaced: true, Kind: "Volume",
		Verbs: []string{"get", "list", "create", "delete"}},
}

// Discovery serves the APIResourceList at GET /v1alpha1, the contract an
// aggregation origin fetches to learn this version's kinds. It names what is
// routable; it does not route, so it lives beside the routes, not above them.
func Discovery(c *gin.Context) {
	c.JSON(http.StatusOK, APIResourceList{GroupVersion: groupVersion, Resources: resources})
}
