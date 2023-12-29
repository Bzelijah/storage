package server

import (
	"errors"
	logger2 "github.com/Bzelijah/storage/internal/logger"
	"github.com/Bzelijah/storage/internal/storage"
	"github.com/labstack/echo/v4"
	"io/ioutil"
	"log"
	"net/http"
)

type Server struct {
	logger logger2.TransactionLogger
}

func New(logger logger2.TransactionLogger) *Server {
	return &Server{
		logger: logger,
	}
}

func (s *Server) KeyValuePutHandler(c echo.Context) error {
	key := c.Param("key")

	value, err := ioutil.ReadAll(c.Request().Body)
	defer c.Request().Body.Close()
	if err != nil {
		return c.String(http.StatusInternalServerError, err.Error())
	}

	err = storage.Put(key, string(value))
	if err != nil {
		return c.String(http.StatusInternalServerError, err.Error())
	}

	s.logger.WritePut(key, string(value))

	log.Printf("PUT key=%s value=%s\n", key, string(value))
	return c.NoContent(http.StatusCreated)
}

func (s *Server) KeyValueGetHandler(c echo.Context) error {
	key := c.Param("key")

	value, err := storage.Get(key)
	if errors.Is(err, storage.ErrorNoSuchKey) {
		return c.String(http.StatusNotFound, err.Error())
	}
	if err != nil {
		return c.String(http.StatusInternalServerError, err.Error())
	}

	log.Printf("GET key=%s\n", key)
	return c.String(http.StatusOK, value)
}

func (s *Server) KeyValueDeleteHandler(c echo.Context) error {
	key := c.Param("key")

	err := storage.Delete(key)
	if err != nil {
		return c.String(http.StatusInternalServerError, err.Error())
	}

	s.logger.WriteDelete(key)

	log.Printf("DELETE key=%s\n", key)
	return c.NoContent(http.StatusOK)
}
