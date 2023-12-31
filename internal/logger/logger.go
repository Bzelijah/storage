package logger

import (
	"bufio"
	"fmt"
	"os"
)

type Event struct {
	Sequence uint64
	// Уникальный порядковый номер записи
	EventType EventType // Выполненное действие
	Key       string
	// Ключ, затронутый этой транзакцией
	Value string
	// Значение для транзакции PUT
}

type FileTransactionLogger struct {
	events       chan<- Event // Канал только для записи; для передачи событий
	errors       <-chan error // Канал только для чтения; для приема ошибок
	lastSequence uint64       // Последний использованный порядковый номер
	file         *os.File     // Местоположение файла журнала
}

func NewFileTransactionLogger(filename string) (TransactionLogger, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0755)
	if err != nil {
		return nil, fmt.Errorf("cannot open transaction log file: %w", err)
	}
	return &FileTransactionLogger{file: file}, nil
}

func (l *FileTransactionLogger) Run() {
	events := make(chan Event, 16)
	// Создать канал событий
	l.events = events
	errors := make(chan error, 1)
	l.errors = errors
	go func() {
		for e := range events {
			l.lastSequence++
			// Создать канал ошибок
			// Извлечь следующее событие Event
			// Увеличить порядковый номер
			_, err := fmt.Fprintf(
				// Записать событие в журнал
				l.file,
				"%d\t%d\t%s\t%s\n",
				l.lastSequence, e.EventType, e.Key, e.Value)
			if err != nil {
				errors <- err
				return
			}
		}
	}()
}

type EventType byte

const (
	_                     = iota // iota == 0; игнорировать нулевое значение
	EventDelete EventType = iota // iota == 1
	EventPut
	// iota == 2; неявное присваивание
)

func (l *FileTransactionLogger) WritePut(key, value string) {
	l.events <- Event{EventType: EventPut, Key: key, Value: value}
}
func (l *FileTransactionLogger) WriteDelete(key string) {
	l.events <- Event{EventType: EventDelete, Key: key}
}
func (l *FileTransactionLogger) Err() <-chan error {
	return l.errors
}

func (l *FileTransactionLogger) ReadEvents() (<-chan Event, <-chan error) {
	scanner := bufio.NewScanner(l.file) // Создать Scanner для чтения l.file
	outEvent := make(chan Event)
	// Небуферизованный канал событий
	outError := make(chan error, 1)
	// Буферизованный канал ошибок
	go func() {
		var e Event
		defer close(outEvent)
		defer close(outError)
		// Закрыть каналы
		// по завершении сопрограммы
		for scanner.Scan() {
			line := scanner.Text()
			if _, err := fmt.Sscanf(line, "%d\t%d\t%s\t%s",
				&e.Sequence, &e.EventType, &e.Key, &e.Value); err != nil {
				outError <- fmt.Errorf("input parse error: %w", err)
				return
			}
			// Проверка целостности!
			// Порядковые номера последовательно увеличиваются?
			if l.lastSequence >= e.Sequence {
				outError <- fmt.Errorf("transaction numbers out of sequence")
				return
			}
			l.lastSequence = e.Sequence // Запомнить последний использованный
			// порядковый номер
			outEvent <- e
			// Отправить событие along
		}
		if err := scanner.Err(); err != nil {
			outError <- fmt.Errorf("transaction log read failure: %w", err)
			return
		}
	}()
	return outEvent, outError
}
