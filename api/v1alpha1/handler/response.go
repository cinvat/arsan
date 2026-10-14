package handler

import (
	"github.com/gin-gonic/gin"
)

// errBody is the single error shape every handler in this version returns, so a
// client parses failures the same way regardless of which endpoint failed.
func errBody(code string, err error) gin.H {
	return gin.H{"code": code, "message": err.Error()}
}

func write(c *gin.Context, status int, code string, err error) {
	c.JSON(status, errBody(code, err))
}
