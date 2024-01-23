package main

import (
	"flag"
	"fmt"
	"log"
	"strconv"
	"time"
	"github.com/WesleiSantos/tangle-client-go/messages"
	"encoding/hex"
	"os"


	"github.com/kyokomi/emoji/v2"
)

const DIRECTORY_NAME = "files"

func main() {
	var amountMessagesInString string
	var amountMessages int
	var index string

	amountMessagesParameter := flag.Int("qtm", -1, "Quantidade de mensagens")
	indexParameter := flag.String("idx", "", "Índice das mensagens")
	timeSleepParameter := flag.Int("tmp", 10, "Tempo de espera entre as mensagens")
	ipParameter := flag.String("ip", "localhost", "IP do nó")
	portParameter := flag.Int("port", 14265, "Porta do nó")
	flag.Parse()

	nodeURL := fmt.Sprintf("http://%s:%d", *ipParameter, *portParameter)
	
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

	if *indexParameter != "" {
		index = *indexParameter
	} else {
		// Se o índice não for passado, obtenha o nome da máquina
		hostname, err := os.Hostname()
		if err != nil {
			fmt.Println("Erro ao obter o nome da máquina:", err)
			return
		}
		index = hostname
	}

	// Exemplo de uso:
	fmt.Println("IP:", *ipParameter)
	fmt.Println("Port:", *portParameter)
	fmt.Println("Topic:", index)

	
	fmt.Println(emoji.Sprint("\n:hourglass:Inciando a publicação."))

	for i := 0; i < amountMessages; i++ {
		// Submitting a message
		start := time.Now()
		fmt.Printf("Time enviado: %s\n", start)
		message := fmt.Sprintf("{\"available\":true,\"avgLoad\":3,\"createdAt\":%d,\"group\":\"group3\",\"lastLoad\":4,\"publishedAt\":%d,\"source\":\"source4\",\"type\":\"LB_STATUS\"}", start.UnixNano(), start.UnixNano())
		id, success := messages.SubmitMessage(nodeURL, index, message, 15)

		if success {
			fmt.Printf("Mensagem %d publicada com sucesso, ID=%s\n", i+1, hex.EncodeToString(id[:]))

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
