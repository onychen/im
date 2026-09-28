package api

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// ──────────────────────────────────────────────────────
// 统一 API 响应格式
// ──────────────────────────────────────────────────────

// ApiResponse 是所有 HTTP JSON 响应的标准信封。
//
// 成功示例：{"code":0,"message":"ok","data":{...}}
// 失败示例：{"code":4001,"message":"不是好友","data":null}
type ApiResponse struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

// ──────────────────────────────────────────────────────
// 通用错误码
// ──────────────────────────────────────────────────────

const (
	CodeSuccess = 0

	// ── 通用 1000~1099 ──
	CodeInternalError = 1000
	CodeMissingParam  = 1001
	CodeInvalidParam  = 1002
	CodeUnauthorized  = 1003

	// ── 认证 1100~1199 ──
	CodeUsernameTooShort = 1101
	CodePasswordTooShort = 1102
	CodeUsernameTaken    = 1103
	CodeUserNotFound     = 1104
	CodeWrongPassword    = 1105
	CodeInvalidToken     = 1106

	// ── 好友 1200~1299 ──
	CodeSelfRequest      = 1201
	CodeAlreadyFriends   = 1202
	CodeFriendBlocked    = 1203
	CodeDuplicateRequest = 1204
	CodeRequestNotFound  = 1205
	CodeNotRequestTarget = 1206
	CodeAlreadyBlocked   = 1207

	// ── 群组 1300~1399 ──
	CodeNotOwnerOrAdmin     = 1301
	CodeGroupNotFound       = 1302
	CodeAlreadyMember       = 1303
	CodeGroupFull           = 1304
	CodeCannotRemoveOwner   = 1305
	CodeCannotLeaveAsOwner  = 1306
	CodeInvalidRole         = 1307
	CodeMemberNotFriend     = 1308
	CodeCannotRemovePeer    = 1309
	CodeGroupMemberNotFound = 1310

	// ── 消息操作 1400~1499 ──
	CodeMsgNotRevocable   = 1401
	CodeMsgRevokeNotOwner = 1402
	CodeMsgDeleteFailed   = 1403

	// ── 朋友圈 1500~1599 ──
	CodeMomentContentEmpty = 1501
	CodeMomentNotFound     = 1502
	CodeNotCommentOwner    = 1503
	CodeInvalidVisibility  = 1504
	CodeCommentNotFound    = 1505
	CodeNotMomentOwner     = 1506

	// ── 设置 1700~1799 ──
	CodeSettingsNotFound = 1701
	CodeMuteConvExists   = 1702
	CodeMuteConvNotFound = 1703
)

// ──────────────────────────────────────────────────────
// 服务层 error string → 响应错误码映射
// ──────────────────────────────────────────────────────

// errorCodeMap 将 service 包定义的 error 常量字符串映射到标准错误码。
// 注意：私聊/群聊消息相关错误码（4001-4003, 5001-5003）沿用 redis 包中
// MapLuaErrToClientCode / MapGroupLuaErrToClientCode 的定义，不在此重复。
var errorCodeMap = map[string]int{
	// 认证
	"用户名必须为3-50个字符": CodeUsernameTooShort,
	"密码必须至少为6个字符":   CodePasswordTooShort,
	"用户名已被占用":       CodeUsernameTaken,
	"用户未找到":         CodeUserNotFound,
	"密码错误":          CodeWrongPassword,
	"刷新令牌无效或已过期":    CodeInvalidToken,

	// 好友
	"不能给自己发送好友请求":     CodeSelfRequest,
	"已经是该用户的好友":       CodeAlreadyFriends,
	"你已拉黑该用户或已被该用户拉黑": CodeFriendBlocked,
	"已存在待处理的好友请求":     CodeDuplicateRequest,
	"好友请求未找到":         CodeRequestNotFound,
	"你不是该好友请求的接收者":    CodeNotRequestTarget,
	"你已经拉黑了该用户":       CodeAlreadyBlocked,

	// 群组
	"只有群主或管理员才能执行此操作": CodeNotOwnerOrAdmin,
	"群组不存在":          CodeGroupNotFound,
	"用户已是群组成员":       CodeAlreadyMember,
	"群组已满（最多 500 人）": CodeGroupFull,
	"无法移除群主":         CodeCannotRemoveOwner,
	"群主无法退出群组；请先转让群主身份或解散群组": CodeCannotLeaveAsOwner,
	"角色值必须为 0（普通成员）或 1（管理员）": CodeInvalidRole,
	"只能邀请好友加入群组":             CodeMemberNotFriend,
	"管理员只能移除普通成员":            CodeCannotRemovePeer,

	// 消息操作
	"消息无法撤回（未找到或已过时效）": CodeMsgNotRevocable,
	"仅发送者可撤回消息":        CodeMsgRevokeNotOwner,
	"删除消息失败":           CodeMsgDeleteFailed,

	// 朋友圈
	"动态内容不能为空":             CodeMomentContentEmpty,
	"内容不能超过 2000 个字符":      CodeInvalidParam,
	"动态未找到":                CodeMomentNotFound,
	"不是该评论的所有者":            CodeNotCommentOwner,
	"可见性必须为 2（好友）或 3（仅自己）": CodeInvalidVisibility,
	"评论未找到":                CodeCommentNotFound,
	"不是该动态的作者":             CodeNotMomentOwner,

	// 设置
	"设置未找到":     CodeSettingsNotFound,
	"会话已被静音":    CodeMuteConvExists,
	"会话不在静音列表中": CodeMuteConvNotFound,
	"会话已不可用":    CodeInvalidParam,

	// 消息服务（msg_service.go 中的错误通过 redis 包映射，这里仅做兜底）
	"不是好友":     4001,
	"你已被对方拉黑":  4002,
	"消息重复":     4003,
	"不是该群组的成员": 5001,
	"你已被禁言":    5002,
	"无法撤回此消息":  1401,
}

// MapErrorCode 将 service 层返回的 error 字符串映射为前端错误码。
// 无法匹配时返回 CodeInternalError。
func MapErrorCode(errStr string) int {
	if code, ok := errorCodeMap[errStr]; ok {
		return code
	}
	return CodeInternalError
}

// ──────────────────────────────────────────────────────
// 便捷响应辅助函数
// ──────────────────────────────────────────────────────

// Success 返回 code=0, message="ok" 的成功响应 (HTTP 200)。
func Success(c *gin.Context, data interface{}) {
	c.JSON(http.StatusOK, ApiResponse{
		Code:    CodeSuccess,
		Message: "ok",
		Data:    data,
	})
}

// SuccessCreated 返回 code=0 的成功响应 (HTTP 201)。
func SuccessCreated(c *gin.Context, data interface{}) {
	c.JSON(http.StatusCreated, ApiResponse{
		Code:    CodeSuccess,
		Message: "ok",
		Data:    data,
	})
}

// SuccessMessage 返回 code=0 的成功响应，仅含提示消息，无 data。
func SuccessMessage(c *gin.Context, message string) {
	c.JSON(http.StatusOK, ApiResponse{
		Code:    CodeSuccess,
		Message: message,
	})
}

// Error 返回自定义错误码和消息的失败响应。
func Error(c *gin.Context, httpStatus int, code int, message string) {
	c.JSON(httpStatus, ApiResponse{
		Code:    code,
		Message: message,
	})
}

// ServiceError 根据 service 层返回的 error 字符串自动映射错误码，
// 并使用合适的 HTTP 状态码。适用于 handler 中 switch err.Error() 分支。
// httpStatus 由调用方显式传入（保证语义准确），code 从 MapErrorCode 自动解析。
func ServiceError(c *gin.Context, httpStatus int, errMsg string) {
	c.JSON(httpStatus, ApiResponse{
		Code:    MapErrorCode(errMsg),
		Message: errMsg,
	})
}

// ──────────────────────────────────────────────────────
// 分页
// ──────────────────────────────────────────────────────

// PaginationMeta 是分页列表响应的元数据。
type PaginationMeta struct {
	Total   int64 `json:"total"`
	Offset  int   `json:"offset"`
	Limit   int   `json:"limit"`
	HasMore bool  `json:"has_more"`
}

// PaginatedData 是带分页信息的数据载荷。
type PaginatedData struct {
	Items      interface{}    `json:"items"`
	Pagination PaginationMeta `json:"pagination"`
}

// PaginatedSuccess 返回 code=0 的分页列表响应。
func PaginatedSuccess(c *gin.Context, items interface{}, total int64, offset, limit int) {
	hasMore := int64(offset+limit) < total
	c.JSON(http.StatusOK, ApiResponse{
		Code:    CodeSuccess,
		Message: "ok",
		Data: PaginatedData{
			Items: items,
			Pagination: PaginationMeta{
				Total:   total,
				Offset:  offset,
				Limit:   limit,
				HasMore: hasMore,
			},
		},
	})
}
