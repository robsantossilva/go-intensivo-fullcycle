package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/robsantossilva/go-intensivo-fullcycle/internal/order/infra/database"
	"github.com/robsantossilva/go-intensivo-fullcycle/internal/order/usecase"
	"github.com/robsantossilva/go-intensivo-fullcycle/pkg/rabbitmq"

	_ "github.com/mattn/go-sqlite3"
)

func main() {

	db, err := sql.Open("sqlite3", "./orders.db")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	repository := database.NewOrderRepository(db)
	uc := usecase.CalculateFinalPriceUseCase{OrderRepository: repository}

	// T2
	ch, err := rabbitmq.OpenChannel()
	if err != nil {
		panic(err)
	}
	defer ch.Close()
	out := make(chan amqp.Delivery) // channel
	//forever := make(chan bool)
	go rabbitmq.Consume(ch, out)

	qtdWorkers := 5
	for workerId := 1; workerId <= qtdWorkers; workerId++ {
		go worker(out, &uc, workerId)
	}

	//<-forever
	http.HandleFunc("/total", func(w http.ResponseWriter, r *http.Request) {
		getTotalUC := usecase.GetTotalUseCase{OrderRepository: repository}
		total, err := getTotalUC.Execute()
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
		}
		json.NewEncoder(w).Encode(total)
	})

	http.ListenAndServe(":8080", nil)
}

func worker(deliveryMessage <-chan amqp.Delivery, uc *usecase.CalculateFinalPriceUseCase, workerId int) {
	for msg := range deliveryMessage {
		var inputDTO usecase.OrderInputDTO
		err := json.Unmarshal(msg.Body, &inputDTO)
		if err != nil {
			panic(err)
		}
		outputDTO, err := uc.Execute(inputDTO)
		if err != nil {
			panic(err)
		}
		msg.Ack(false)
		fmt.Printf("worker %d has processed order %s\n", workerId, outputDTO.ID)
		time.Sleep(1 * time.Second)
	}
}
