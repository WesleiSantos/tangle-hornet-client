package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"time"
	"sync"
	"os"
	"encoding/csv"
	"strconv"
	"encoding/hex"


	"context"
	"errors"

	//"github.com/WesleiSantos/tangle-client-go/messages"
	"github.com/kyokomi/emoji/v2"
	iotago "github.com/iotaledger/iota.go/v2"

)

const DIRECTORY_NAME = "files"

var i int = 0

type Message struct {
	ID             string   `json:"id"`
	NetworkID      uint64   `json:"networkId"`
	Nonce          uint64   `json:"nonce"`
	Payload        struct {
		Data  string `json:"data"`
		Index string `json:"index"`
	} `json:"payload"`
}

type MessageInfo struct {
	StartTime   time.Time
	ReceiveTime time.Time
}

type MessageWithTimestamp struct {
	Message           string
	ExactReceiveTime  time.Time
}

type Messagen struct {
	Index string `json:"index"`
	Data  string `json:"data"`
}

// Get a message on the node by a given message ID.
func getMessageByMessageID(nodeUrl string, messageIdHex string) (*iotago.Message, error) {
	node := iotago.NewNodeHTTPAPIClient(nodeUrl)

	messageId, err := iotago.MessageIDFromHexString(messageIdHex)
	if err != nil {
		return &iotago.Message{}, errors.New("unable to convert message ID from hex to message ID representation")
	}

	messageReturned, err := node.MessageByMessageID(context.Background(), messageId)
	if err != nil {
		return &iotago.Message{}, errors.New("unable to get message by given message ID")
	}

	return messageReturned, nil
}

func GetLastHourMessagesByIndex(nodeUrl string, index string, maxMessages int) ([]*iotago.Message, error) {
	node := iotago.NewNodeHTTPAPIClient(nodeUrl)

	msgIdsResponse, err := node.MessageIDsByIndex(
		context.Background(),
		[]byte(index),
	)

	if err != nil {
		return nil, errors.New("unable to get message IDs")
	}

	var i uint32
	var messages []*iotago.Message
	if msgIdsResponse.Count > 0 {
		for i = 0; i < msgIdsResponse.Count; i++ {
			var message *iotago.Message

			messageReturned, err := getMessageByMessageID(nodeUrl, msgIdsResponse.MessageIDs[i])

			if err != nil {
				log.Println(err)
			} else {
				message = messageReturned
			}

			var data map[string]interface{}
			var createdAtInt64 int64
			indexationPayload := messageReturned.Payload.(*iotago.Indexation)

			err = json.Unmarshal([]byte(string(indexationPayload.Data)), &data)

			if err != nil {
				return nil, errors.New("error trying to decode JSON")
			}

			if createdAt, ok := data["createdAt"].(float64); ok {
				// One-hour time limit
				timeLimit := time.Now().UnixNano() - int64(10 * time.Second)

				createdAtInt64 = int64(createdAt)
				if createdAtInt64 >= timeLimit {
					messages = append(messages, message)
				}

				if len(messages) == maxMessages {
					break
				}
			} else {
				return nil, errors.New("error, this JSON doesn't have 'createdAt' parameter")
			}
		}

		if (len(messages) == 0) {
			log.Println("No messages have been created in the last hour.")
		}
	} else {
		log.Println("No messages with this index were found.")
	}

	return messages, nil
}

func convertToMessageStruct(iotaMessage *iotago.Message) (*Message, error) {
	id, err := iotaMessage.ID() 
	if err != nil {
		return nil, err
	}
	idHex := hex.EncodeToString(id[:])	
	
	message := &Message{
		ID:             idHex,
		NetworkID:      iotaMessage.NetworkID,
		Nonce:          iotaMessage.Nonce,
	}

	indexationPayload := iotaMessage.Payload.(*iotago.Indexation)
	message.Payload.Data = string(indexationPayload.Data)
	message.Payload.Index = string(indexationPayload.Index)

	return message, nil
}

func receiveMessages(nodeURL string, index string, messageChan chan MessageWithTimestamp, stopChan chan struct{}, waitGroup *sync.WaitGroup) {
	defer waitGroup.Done()
	fmt.Println(emoji.Sprint("\n:hourglass:Inciando a leitura das mensagens."))
	fmt.Println("Node URL:", nodeURL)
	fmt.Println("Index:", index)
	for {
		select {
		case <-stopChan:
			close(messageChan)
			fmt.Println(emoji.Sprint("\n:hourglass:Finalizando a leitura das mensagens."))
			return // Encerrar a goroutine quando receber sinal de parada
		default:
			// Receive message from server
			messages, err := GetLastHourMessagesByIndex(nodeURL, index, 5)
			if err != nil {
				log.Printf("Failed to receive message: %s", err)
				continue
			}
			// Adicionar timestamp exato
			exactReceiveTime := time.Now()

			// Convert each message to MessageWithTimestamp and send to channel
			for _, message := range messages {
				messageStruct, err := convertToMessageStruct(message)
				if err != nil {
					log.Printf("Failed to convert message to struct: %s", err)
					continue
				}
			
				// Convert Message struct to JSON string
				messageJSON, err := json.Marshal(messageStruct)
				if err != nil {
					log.Printf("Failed to convert message to JSON: %s", err)
					continue
				}
			
				// Send message to channel for saving
				messageChan <- MessageWithTimestamp{Message: string(messageJSON), ExactReceiveTime: exactReceiveTime}
			}

		}
	}
}

func saveToMap(messageChan chan MessageWithTimestamp, messageMap *sync.Map, size int, done chan struct{}) {
	elementCount := 0
	mutex := sync.Mutex{} // Adicionando mutex para garantir operações seguras no elementoCount

	for {
		// Receive message from channel
		msgWithTimestamp, more := <-messageChan
		if !more {
			close(done) // Fechar o canal de conclusão quando o canal de mensagens estiver fechado
			return
		}
		
		var message Message
		err := json.Unmarshal([]byte(msgWithTimestamp.Message), &message)
		if err != nil {
			log.Printf("Received message with unexpected format: %s", msgWithTimestamp)
			fmt.Println("Error:", err)
			return
		} else {
			// Acesse o campo "publishedAt" dentro de "payload"
			var data map[string]interface{}
			if err := json.Unmarshal([]byte(message.Payload.Data), &data); err != nil {
			    log.Printf("Failed to parse 'data' field: %s", err)
			    continue
			}
			
			// Agora você pode acessar o campo "publishedAt" dentro de "data"
			startTimeUnix := data["publishedAt"].(float64)

			startTime := time.Unix(int64(startTimeUnix)/int64(time.Second), (int64(startTimeUnix)%int64(time.Second)))
			fmt.Printf("Time Enviado: %s Time Recebido: %s\n", startTime, msgWithTimestamp.ExactReceiveTime)

			 // Verificar se a chave já existe antes de inserir
			_, loaded := messageMap.LoadOrStore(message.ID, MessageInfo{StartTime: startTime, ReceiveTime: msgWithTimestamp.ExactReceiveTime})

			// Incrementar a contagem de elementos se a chave não existir previamente
			if !loaded {
				mutex.Lock()
				elementCount++
				mutex.Unlock()
			}

			// Verificar se o tamanho do mapa atingiu ou excedeu o valor especificado
			if elementCount >= size {
				close(done) // Sinalizar para encerrar
				return
			}

		} 
	}
}

func saveFile(messageMap *sync.Map) {
	// Verificar se o diretório existe, se não, criá-lo
	if _, err := os.Stat(DIRECTORY_NAME); os.IsNotExist(err) {
		if err := os.MkdirAll(DIRECTORY_NAME, 0755); err != nil {
			log.Fatal(err)
		}
	}

	files, err := os.ReadDir(DIRECTORY_NAME)
	if err != nil {
		log.Fatal(err)
	}

	fileName := fmt.Sprintf("tangle-hornet-reading-time_%d.csv", len(files))
	filePath := fmt.Sprintf("%s/%s", DIRECTORY_NAME, fileName)

	file, err := os.Create(filePath)
	if err != nil {
		log.Fatal(err)
	}

	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Writing header to csv file.
	header := []string{"Índice", "Tempo de consulta (s)"}
	if err := writer.Write(header); err != nil {
		log.Fatal(err)
	}

	// Iterar sobre o mapa para calcular e salvar o tempo de consulta de cada mensagem
	messageMap.Range(func(key, value interface{}) bool {
		messageInfo := value.(MessageInfo)

		// Calcular o tempo de consulta para esta mensagem
		elapsed := messageInfo.ReceiveTime.Sub(messageInfo.StartTime)
		elapsedInString := strconv.FormatFloat(elapsed.Seconds(), 'f', -1, 64)

		// Escrever dados no arquivo CSV
		i++
		row := []string{strconv.Itoa(i), elapsedInString}
		if err := writer.Write(row); err != nil {
			log.Fatal(err)
		}

		return true
	})

	writer.Flush()
}

func main() {
	// Definir as flags
	ip := flag.String("ip", "localhost", "IP address of the ZMQ server")
	port := flag.String("port", "14265", "Port of the ZMQ server")
	size := flag.Int("size", 30, "Size of the message map")
	indexParameter := flag.String("idx", "LB_STATUS", "Índice das mensagens")

	flag.Parse()

	// Exemplo de uso:
	fmt.Println("IP:", *ip)
	fmt.Println("Port:", *port)
	fmt.Println("Topic:", *indexParameter)

	nodeURL := fmt.Sprintf("http://%s:%s", *ip, *port)

	var wg sync.WaitGroup
	
	// Canal para transmitir mensagens da função receiveMessages para a goroutine saveToMap
	messageChan := make(chan MessageWithTimestamp)

	// Canal para sinalizar a parada
	stopChan := make(chan struct{})

	// Canal para sinalizar a conclusão
	done := make(chan struct{})

	// Mapa seguro para armazenar as mensagens
	var messageMap sync.Map

	// Iniciar a goroutine para salvar mensagens no mapa
	go saveToMap(messageChan, &messageMap,  *size, done)

	// Iniciar a thread para receber mensagens
	wg.Add(1)
	go receiveMessages(nodeURL, *indexParameter, messageChan, stopChan, &wg)

	// Aguardar até que a conclusão seja sinalizada
	<-done
	// Sinalize a parada da goroutine de recebimento
	close(stopChan)

	saveFile(&messageMap)

	return 
}
