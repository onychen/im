package middleware

import (
	"encoding/json"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
)

func TestJWTAuthMiddleware_ValidToken(t *testing.T) {
	gin.SetMode(gin.TestMode)

	token, err := GenerateAccessToken(1, "testuser", "test-secret", 2)
	assert.NoError(t, err)

	r := gin.New()
	r.Use(JWTAuthMiddleware("test-secret"))
	r.GET("/test", func(c *gin.Context) {
		userID := c.GetInt64("userID")
		username := c.GetString("username")
		c.JSON(200, gin.H{"userID": userID, "username": username})
	})

	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, 200, w.Code)
}

func TestJWTAuthMiddleware_InvalidToken(t *testing.T) {
	gin.SetMode(gin.TestMode)

	r := gin.New()
	r.Use(JWTAuthMiddleware("test-secret"))
	r.GET("/test", func(c *gin.Context) {
		c.JSON(200, gin.H{})
	})

	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("Authorization", "Bearer invalid-token-here")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, 401, w.Code)
	var response authErrorResponse
	assert.NoError(t, json.Unmarshal(w.Body.Bytes(), &response))
	assert.Equal(t, codeUnauthorized, response.Code)
	assert.Equal(t, "无效令牌", response.Message)
}

func TestJWTAuthMiddleware_MissingToken(t *testing.T) {
	gin.SetMode(gin.TestMode)

	r := gin.New()
	r.Use(JWTAuthMiddleware("test-secret"))
	r.GET("/test", func(c *gin.Context) {
		c.JSON(200, gin.H{})
	})

	req := httptest.NewRequest("GET", "/test", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, 401, w.Code)
	var response authErrorResponse
	assert.NoError(t, json.Unmarshal(w.Body.Bytes(), &response))
	assert.Equal(t, codeUnauthorized, response.Code)
	assert.Equal(t, "缺少令牌", response.Message)
}

func TestJWTAuthMiddleware_QueryParamToken(t *testing.T) {
	gin.SetMode(gin.TestMode)

	token, err := GenerateAccessToken(42, "wsuser", "ws-secret", 2)
	assert.NoError(t, err)

	r := gin.New()
	r.Use(JWTAuthMiddleware("ws-secret"))
	r.GET("/ws", func(c *gin.Context) {
		userID := c.GetInt64("userID")
		username := c.GetString("username")
		c.JSON(200, gin.H{"userID": userID, "username": username})
	})

	req := httptest.NewRequest("GET", "/ws?token="+token, nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, 200, w.Code)
}

func TestParseToken_ValidToken(t *testing.T) {
	token, err := GenerateAccessToken(10, "parseuser", "parse-secret", 2)
	assert.NoError(t, err)

	parsedToken, claims, err := ParseToken(token, "parse-secret")
	assert.NoError(t, err)
	assert.NotNil(t, parsedToken)
	assert.True(t, parsedToken.Valid)
	assert.Equal(t, int64(10), claims.UserID)
	assert.Equal(t, "parseuser", claims.Username)
}

func TestParseToken_InvalidToken(t *testing.T) {
	_, _, err := ParseToken("invalid-token-string", "some-secret")
	assert.Error(t, err)
}

func TestParseToken_WrongSecret(t *testing.T) {
	token, err := GenerateAccessToken(5, "user5", "secret-a", 2)
	assert.NoError(t, err)

	_, _, err = ParseToken(token, "secret-b")
	assert.Error(t, err)
}

func TestGenerateAccessToken(t *testing.T) {
	token, err := GenerateAccessToken(100, "alice", "mysecret", 2)
	assert.NoError(t, err)
	assert.NotEmpty(t, token)
}

func TestGenerateRefreshToken(t *testing.T) {
	token, err := GenerateRefreshToken(100, "mysecret", 7)
	assert.NoError(t, err)
	assert.NotEmpty(t, token)
}
