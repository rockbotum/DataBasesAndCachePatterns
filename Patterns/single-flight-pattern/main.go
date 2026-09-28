package singleflightpattern

import (
	"context"
)

type Cache interface {
	Get(ctx context.Context, key string) (any, error)
	Set(ctx context.Context, key string, value any) error
}

type Database interface {
	Query(ctx context.Context, query string, args ...any) (any, error)
}

// Стандартная реализация: Запрос -> Кеш -> БД
func GetUserBalance(ctx context.Context, userID string, cache Cache, db Database) (any, error) {
	// читаем кеш
	value, err := cache.Get(ctx, userID)
	if err == nil {
		return value, nil
	}

	// если в кеше нет, читаем БД
	const query = "SELECT balance FROM users WHERE user_id = ?"
	value, err = db.Query(ctx, query, userID)
	if err != nil {
		return nil, err
	}

	// пишем в кеш (ошибку игнорируем, можно залогировать)
	if err := cache.Set(ctx, userID, value); err != nil {
		// log.Warn("cache set error", err)
	}

	return value, nil
}

// Cache-aside + singleflight (конкурентность)
func GetUserName(ctx context.Context, userID string, cache Cache, db Database) (any, error) {
	// поиск в кеше
	value, err := cache.Get(ctx, userID)
	if err == nil {
		return value, nil
	}

	const query = "SELECT name FROM users WHERE user_id = ?"

	// дедупликация запросов к БД для одного userID
	return NewSingleFlight().Do(ctx, "user:name:"+userID, func(ctx context.Context) (any, error) {
		value, err := db.Query(ctx, query, userID)
		if err != nil {
			return nil, err
		}

		if err := cache.Set(ctx, "user:name:"+userID, value); err != nil {
			// log.Warn("cache set error", err)
		}

		return value, nil
	})
}
