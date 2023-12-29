package main

import (
	"fmt"
	logger2 "github.com/Bzelijah/storage/internal/logger"
	"github.com/Bzelijah/storage/internal/server"
	"github.com/Bzelijah/storage/internal/storage"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
)

var logger logger2.TransactionLogger

func InitializeTransactionLog() (logger2.TransactionLogger, error) {
	var err error
	logger, err = logger2.NewFileTransactionLogger("/home/bzelijah/Desktop/books/practice/Облачный Go/storage/transaction.log")
	if err != nil {
		return nil, fmt.Errorf("failed to create event logger: %w", err)
	}
	events, errors := logger.ReadEvents()
	e, ok := logger2.Event{}, true
	for ok && err == nil {
		select {
		case err, ok = <-errors: // Получает ошибки
		case e, ok = <-events:
			switch e.EventType {
			case logger2.EventDelete:
				// Получено событие DELETE!
				err = storage.Delete(e.Key)
			case logger2.EventPut:
				// Получено событие PUT!
				err = storage.Put(e.Key, e.Value)
			}
		}
	}
	logger.Run()
	return logger, err
}

func main() {
	e := echo.New()

	logger, err := InitializeTransactionLog()
	if err != nil {
		e.Logger.Error(err)
		return
	}

	e.Use(middleware.Logger())

	handlers := server.New(logger)

	e.GET("/v1/:key", handlers.KeyValueGetHandler)
	e.PUT("/v1/:key", handlers.KeyValuePutHandler)
	e.DELETE("/v1/:key", handlers.KeyValueDeleteHandler)

	e.Logger.Fatal(e.Start(":8080"))
}
