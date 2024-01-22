package main

import (
	"flag"
	"fmt"
	"log"
	"strconv"
	"time"
	"github.com/WesleiSantos/tangle-client-go/messages"


	"github.com/kyokomi/emoji/v2"
)

const DIRECTORY_NAME = "files"

func main() {
	var amountMessagesInString string
	var amountMessages int
	var index string

	nodeURL := "http://localhost:14265"

	amountMessagesParameter := flag.Int("qtm", -1, "Quantidade de mensagens")
	indexParameter := flag.String("idx", "", "Índice das mensagens")
	timeSleepParameter := flag.Int("tmp", 10, "Tempo de espera entre as mensagens")
	flag.Parse()
	
	if (*amountMessagesParameter == -1) {
		var err error
		fmt.Print("Digite quantas mensagens você quer gerar: ")
		fmt.Scanln(&amountMessagesInString)

		amountMessages, err = strconv.Atoi(amountMessagesInString)
		if err != nil {
			log.Fatal(err)
		}
	} else {
		amountMessages = *amountMessagesParameter
	}

	if amountMessages < 0 {
		log.Fatal("invalid amount of messages")
	}

	if (*indexParameter == "") {
		fmt.Print("Digite o índice para as mensagens: ")
		fmt.Scanln(&index)
	} else {
		index = *indexParameter
	}

	
	fmt.Println(emoji.Sprint("\n:hourglass:Inciando a publicação."))

	for i := 0; i < amountMessages; i++ {
		// Submitting a message
		start := time.Now()
		fmt.Printf("Time enviado: %s\n", start)
		message := fmt.Sprintf("{\"available\":true,\"avgLoad\":3,\"createdAt\":%d,\"group\":\"group3\",\"lastLoad\":4,\"publishedAt\":%d,\"source\":\"source4\",\"type\":\"LB_STATUS\"}", start.UnixNano(), start.UnixNano())
		_, success := messages.SubmitMessage(nodeURL, index, message, 15)

		if success {
			fmt.Printf("Mensagem %d publicada com sucesso\n", i+1)

			if i == amountMessages/4 {
				fmt.Println(emoji.Sprint(":heavy_check_mark: 25% das mensagens já foram publicadas e consultadas."))
			} else if i == amountMessages/2 {
				fmt.Println(emoji.Sprint(":heavy_check_mark: 50% das mensagens já foram publicadas e consultadas."))
			} else if i == amountMessages/4+amountMessages/2 {
				fmt.Println(emoji.Sprint(":heavy_check_mark: 75% das mensagens já foram publicadas e consultadas."))
			}
		}
		time.Sleep(time.Duration(*timeSleepParameter) * time.Second)
	}

}
