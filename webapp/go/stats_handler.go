package main

import (
	"database/sql"
	"errors"
	"log"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/labstack/echo/v4"
)

type LivestreamStatistics struct {
	Rank           int64 `json:"rank"`
	ViewersCount   int64 `json:"viewers_count"`
	TotalReactions int64 `json:"total_reactions"`
	TotalReports   int64 `json:"total_reports"`
	MaxTip         int64 `json:"max_tip"`
}

type LivestreamRankingEntry struct {
	LivestreamID int64
	Score        int64
}
type LivestreamRanking []LivestreamRankingEntry

func (r LivestreamRanking) Len() int      { return len(r) }
func (r LivestreamRanking) Swap(i, j int) { r[i], r[j] = r[j], r[i] }
func (r LivestreamRanking) Less(i, j int) bool {
	if r[i].Score == r[j].Score {
		return r[i].LivestreamID < r[j].LivestreamID
	} else {
		return r[i].Score < r[j].Score
	}
}

type UserStatistics struct {
	Rank              int64  `json:"rank"`
	ViewersCount      int64  `json:"viewers_count"`
	TotalReactions    int64  `json:"total_reactions"`
	TotalLivecomments int64  `json:"total_livecomments"`
	TotalTip          int64  `json:"total_tip"`
	FavoriteEmoji     string `json:"favorite_emoji"`
}

type UserRankingEntry struct {
	Username string
	Score    int64
}
type UserRanking []UserRankingEntry

func (r UserRanking) Len() int      { return len(r) }
func (r UserRanking) Swap(i, j int) { r[i], r[j] = r[j], r[i] }
func (r UserRanking) Less(i, j int) bool {
	if r[i].Score == r[j].Score {
		return r[i].Username < r[j].Username
	} else {
		return r[i].Score < r[j].Score
	}
}

func getUserStatisticsHandler(c echo.Context) error {
	since := time.Now()
	ctx := c.Request().Context()

	if err := verifyUserSession(c); err != nil {
		// echo.NewHTTPErrorが返っているのでそのまま出力
		return err
	}

	username := c.Param("username")
	// ユーザごとに、紐づく配信について、累計リアクション数、累計ライブコメント数、累計売上金額を算出
	// また、現在の合計視聴者数もだす

	tx, err := dbConn.BeginTxx(ctx, nil)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to begin transaction: "+err.Error())
	}
	defer tx.Rollback()

	var userID int64
	if err := tx.GetContext(ctx, &userID, "SELECT id FROM users WHERE name = ?", username); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return echo.NewHTTPError(http.StatusBadRequest, "not found user that has the given username")
		} else {
			return echo.NewHTTPError(http.StatusInternalServerError, "failed to get user: "+err.Error())
		}
	}
	log.Printf("Got user id=%d name=%s (%.2fs)", userID, username, time.Since(since).Seconds())

	// ランク算出
	var users []*struct {
		ID        int64  `db:"id"`
		Name      string `db:"name"`
		reactions int64
		tips      int64
	}
	if err := tx.SelectContext(ctx, &users, "SELECT id, name FROM users"); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to get users: "+err.Error())
	}
	userMap := make(map[int64]*struct {
		name      string
		reactions int64
		tips      int64
	}, len(users))
	for _, u := range users {
		userMap[u.ID] = &struct {
			name      string
			reactions int64
			tips      int64
		}{name: u.Name}
	}
	log.Printf("Got users len=%d (%.2fs)", len(userMap), time.Since(since).Seconds())

	var reactions []struct {
		UserId int64 `db:"user_id"`
		Count  int64 `db:"count"`
	}
	query := `
		SELECT u.id AS user_id, COUNT(*) AS count FROM users u
		INNER JOIN livestreams l ON l.user_id = u.id
		INNER JOIN reactions r ON r.livestream_id = l.id
        GROUP BY u.id`
	if err := tx.SelectContext(ctx, &reactions, query); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to count reactions: "+err.Error())
	}
	for _, r := range reactions {
		userMap[r.UserId].reactions = r.Count
	}
	log.Printf("Got reactions len=%d (%.2fs)", len(reactions), time.Since(since).Seconds())

	var tips []struct {
		UserId int64 `db:"user_id"`
		Count  int64 `db:"count"`
	}
	query = `
		SELECT u.id AS user_id, IFNULL(SUM(l2.tip), 0) AS count FROM users u
		INNER JOIN livestreams l ON l.user_id = u.id	
		INNER JOIN livecomments l2 ON l2.livestream_id = l.id
        GROUP BY u.id`
	if err := tx.SelectContext(ctx, &tips, query); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to count tips: "+err.Error())
	}
	for _, t := range tips {
		userMap[t.UserId].tips = t.Count
	}
	log.Printf("Got tips len=%d (%.2fs)", len(tips), time.Since(since).Seconds())

	ranking := make(UserRanking, 0, len(userMap))
	for _, u := range userMap {
		score := u.reactions + u.tips
		ranking = append(ranking, UserRankingEntry{
			Username: u.name,
			Score:    score,
		})
	}
	sort.Sort(ranking)
	log.Printf("sorted ranking (%.2fs)", time.Since(since).Seconds())

	var rank int64 = 1
	for i := len(ranking) - 1; i >= 0; i-- {
		entry := ranking[i]
		if entry.Username == username {
			break
		}
		rank++
	}
	log.Printf("calculated ranking (%.2fs)", time.Since(since).Seconds())

	var livestreamIDs []int64
	if err := tx.SelectContext(ctx, &livestreamIDs, "SELECT id FROM livestreams WHERE user_id = ?", userID); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to get livestreams: "+err.Error())
	}
	log.Printf("got livestreams (%.2fs)", time.Since(since).Seconds())

	// ライブコメント数、チップ合計
	var livecomments struct {
		Count    int64 `db:"count"`
		TotalTip int64 `db:"total_tip"`
	}
	query, params, err := sqlx.In("SELECT COUNT(*) as count, IFNULL(SUM(tip), 0) as total_tip FROM livecomments WHERE livestream_id IN (?)", livestreamIDs)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to create livecomments query: "+err.Error())
	}
	if err := tx.GetContext(ctx, &livecomments, query, params...); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to get livecomments: "+err.Error())
	}
	totalLivecomments := livecomments.Count
	totalTip := livecomments.TotalTip
	log.Printf("counted livecomments (%.2fs)", time.Since(since).Seconds())

	// 合計視聴者数
	var viewersCount int64
	query, params, err = sqlx.In("SELECT COUNT(*) FROM livestream_viewers_history WHERE livestream_id IN (?)", livestreamIDs)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to create livestream_view_history query: "+err.Error())
	}
	if err := tx.GetContext(ctx, &viewersCount, query, params...); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to get livestream_view_history: "+err.Error())
	}
	log.Printf("counted viewers (%.2fs)", time.Since(since).Seconds())

	// お気に入り絵文字
	var favoriteEmoji string
	query = `
	SELECT r.emoji_name
	FROM users u
	INNER JOIN livestreams l ON l.user_id = u.id
	INNER JOIN reactions r ON r.livestream_id = l.id
	WHERE u.name = ?
	GROUP BY emoji_name
	ORDER BY COUNT(*) DESC, emoji_name DESC
	LIMIT 1
	`
	if err := tx.GetContext(ctx, &favoriteEmoji, query, username); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to find favorite emoji: "+err.Error())
	}
	log.Printf("got emoji_name (%.2fs)", time.Since(since).Seconds())

	stats := UserStatistics{
		Rank:              rank,
		ViewersCount:      viewersCount,
		TotalReactions:    userMap[userID].reactions,
		TotalLivecomments: totalLivecomments,
		TotalTip:          totalTip,
		FavoriteEmoji:     favoriteEmoji,
	}
	return c.JSON(http.StatusOK, stats)
}

func getLivestreamStatisticsHandler(c echo.Context) error {
	ctx := c.Request().Context()

	if err := verifyUserSession(c); err != nil {
		return err
	}

	id, err := strconv.Atoi(c.Param("livestream_id"))
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "livestream_id in path must be integer")
	}
	livestreamID := int64(id)

	tx, err := dbConn.BeginTxx(ctx, nil)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to begin transaction: "+err.Error())
	}
	defer tx.Rollback()

	var livestramID int64
	if err := tx.GetContext(ctx, &livestramID, "SELECT id FROM livestreams WHERE id = ?", livestreamID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return echo.NewHTTPError(http.StatusBadRequest, "cannot get stats of not found livestream")
		} else {
			return echo.NewHTTPError(http.StatusInternalServerError, "failed to get livestream: "+err.Error())
		}
	}

	// ランク算出
	var livestreamIDs []int64
	if err := tx.SelectContext(ctx, &livestreamIDs, "SELECT id FROM livestreams"); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to get livestreams: "+err.Error())
	}
	livestreamMap := make(map[int64]*struct {
		reactions int64
		totalTips int64
	}, len(livestreamIDs))
	for _, id := range livestreamIDs {
		livestreamMap[id] = &struct {
			reactions int64
			totalTips int64
		}{}
	}

	var reactions []struct {
		LivestreamID int64 `db:"livestream_id"`
		Count        int64 `db:"count"`
	}
	query := "SELECT l.id AS livestream_id, COUNT(*) AS count FROM livestreams l INNER JOIN reactions r ON l.id = r.livestream_id GROUP BY l.id"
	if err := tx.SelectContext(ctx, &reactions, query); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to count reactions: "+err.Error())
	}
	for _, r := range reactions {
		livestreamMap[r.LivestreamID].reactions = r.Count
	}

	var totalTips []struct {
		LivestreamID int64 `db:"livestream_id"`
		Count        int64 `db:"count"`
	}
	query = "SELECT l.id AS livestream_id, IFNULL(SUM(l2.tip), 0) AS count FROM livestreams l INNER JOIN livecomments l2 ON l.id = l2.livestream_id GROUP BY l.id"
	if err := tx.SelectContext(ctx, &totalTips, query); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to count tips: "+err.Error())
	}
	for _, t := range totalTips {
		livestreamMap[t.LivestreamID].totalTips = t.Count
	}

	ranking := make(LivestreamRanking, 0, len(livestreamIDs))
	for id, v := range livestreamMap {
		score := v.reactions + v.totalTips
		ranking = append(ranking, LivestreamRankingEntry{
			LivestreamID: id,
			Score:        score,
		})
	}
	sort.Sort(ranking)

	var rank int64 = 1
	for i := len(ranking) - 1; i >= 0; i-- {
		entry := ranking[i]
		if entry.LivestreamID == livestreamID {
			break
		}
		rank++
	}

	// 視聴者数算出
	var viewersCount int64
	if err := tx.GetContext(ctx, &viewersCount, `SELECT COUNT(*) FROM livestreams l INNER JOIN livestream_viewers_history h ON h.livestream_id = l.id WHERE l.id = ?`, livestreamID); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to count livestream viewers: "+err.Error())
	}

	// 最大チップ額
	var maxTip int64
	if err := tx.GetContext(ctx, &maxTip, `SELECT IFNULL(MAX(tip), 0) FROM livestreams l INNER JOIN livecomments l2 ON l2.livestream_id = l.id WHERE l.id = ?`, livestreamID); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to find maximum tip livecomment: "+err.Error())
	}

	// リアクション数
	var totalReactions int64
	if err := tx.GetContext(ctx, &totalReactions, "SELECT COUNT(*) FROM livestreams l INNER JOIN reactions r ON r.livestream_id = l.id WHERE l.id = ?", livestreamID); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to count total reactions: "+err.Error())
	}

	// スパム報告数
	var totalReports int64
	if err := tx.GetContext(ctx, &totalReports, `SELECT COUNT(*) FROM livestreams l INNER JOIN livecomment_reports r ON r.livestream_id = l.id WHERE l.id = ?`, livestreamID); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to count total spam reports: "+err.Error())
	}

	if err := tx.Commit(); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to commit: "+err.Error())
	}

	return c.JSON(http.StatusOK, LivestreamStatistics{
		Rank:           rank,
		ViewersCount:   viewersCount,
		MaxTip:         maxTip,
		TotalReactions: totalReactions,
		TotalReports:   totalReports,
	})
}
