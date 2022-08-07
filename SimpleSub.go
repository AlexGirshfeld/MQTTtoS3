package main

import (
	"encoding/binary"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"gopkg.in/yaml.v3"
	"io/ioutil"
	"log"
	"time"
)

type Conf struct {
	Broker        string              `yaml:"broker"`
	Buckets       map[string][]string `yaml:"buckets"`
	StopCondition string              `yaml:"stopCondition"`
	MaxSize       int                 `yaml:"maxSize"`
}

var messagePubHandler mqtt.MessageHandler = func(client mqtt.Client, msg mqtt.Message) {
	fmt.Printf("Message %s received on topic %s\n", msg.Payload(), msg.Topic())
}

func messageHandlerCreator(c chan mqtt.Message) func(mqtt.Client, mqtt.Message) {
	// gets a channel, and return a function that send messages to this channel
	return func(client mqtt.Client, msg mqtt.Message) {
		c <- msg
	}
}

func uploadToS3(bucket string, time string, payload []byte, headerSize int, uploader *s3manager.Uploader) {

	// check code that just save the object as a file
	err := ioutil.WriteFile("file", payload, 0644)
	if err != nil {
		println("didn't wrote file")
	} else {
		fmt.Printf("file uploaded")
	}

	// actual code to upload to s3

	//m := make(map[string]*string)
	//str := strconv.Itoa(headerSize)
	//m["x-amz-meta-mqtt-header-size"] = &str
	//result, err := uploader.Upload(&s3manager.UploadInput{
	//	Bucket:   aws.String(bucket),
	//	Key:      aws.String(time),
	//	Body:     bytes.NewReader(payload),
	//	Metadata: m,
	//})
	//if err != nil {
	//	fmt.Printf("failed to upload file, %v\n", err)
	//}
	//fmt.Printf("Message at time %s was uploaded to bucket %s\n ID = %s, location = %s",
	//	time, bucket, result.UploadID, result.Location)
}

const (
	timeInSeconds int = 0
	sizeInBytes       = 1
	sizeInLength      = 2
)

func mqttPayloadArrToS3Payload(payloadArr [][]byte, unixTimeArray []uint32) ([]byte, int) {
	//  the header is an array of:
	//  4 bytes offset of the message from the beginning of the object
	//  4 bytes unix timestamp
	//  so each payload takes 8 bytes in the header
	headerSize := 8 * len(payloadArr)
	header := make([]byte, headerSize)
	var body []byte
	offset := uint32(headerSize)
	temp1 := make([]byte, 4)
	temp2 := make([]byte, 4)

	for index, payload := range payloadArr {
		binary.LittleEndian.PutUint32(temp1, offset)
		binary.LittleEndian.PutUint32(temp2, unixTimeArray[index])
		for i := 0; i < 4; i++ {
			header[index*8+i] = temp1[i]
			header[index*8+i+4] = temp2[i]
		}
		body = append(body, payload...)
		offset += uint32(len(payload))
	}

	finalPayload := make([]byte, len(header)+len(body))
	copy(finalPayload[:headerSize], header)
	copy(finalPayload[headerSize:], body)
	fmt.Printf("header size is %v \nbody size is %v\n so the file size is %v", headerSize, len(body), len(finalPayload))
	return finalPayload, headerSize
}

func receiver(bucket string, channel chan mqtt.Message, uploader *s3manager.Uploader, stopCondition int,
	maxSize int) {
	var payloadArr [][]byte
	size := 0
	var unixTimeArr []uint32
	startTime := time.Now()

	for msg := range channel {
		fmt.Printf("Message %s received on topic %s and will be uploaded to bucket %s\n",
			msg.Payload(), msg.Topic(), bucket)
		unixTimeArr = append(unixTimeArr, uint32(time.Now().Unix()))
		payloadArr = append(payloadArr, msg.Payload())
		switch stopCondition {
		case sizeInLength:
			size += 1
			if size >= maxSize {
				size = 0
				payload, headerSize := mqttPayloadArrToS3Payload(payloadArr, unixTimeArr)
				uploadToS3(bucket, time.Now().String(), payload, headerSize, uploader)
			}
		case sizeInBytes:
			size += len(msg.Payload())
			// notice: can be also len(payload_arr) >= maxSize
			if size >= maxSize {
				size = 0
				payload, headerSize := mqttPayloadArrToS3Payload(payloadArr, unixTimeArr)
				uploadToS3(bucket, time.Now().String(), payload, headerSize, uploader)
			}
		case timeInSeconds:
			timeInSecond := int(startTime.Sub(time.Now()) / time.Second)
			if timeInSecond >= maxSize {
				startTime = time.Now()
				payload, headerSize := mqttPayloadArrToS3Payload(payloadArr, unixTimeArr)
				uploadToS3(bucket, time.Now().String(), payload, headerSize, uploader)
			}
		}

	}
}

var connectHandler mqtt.OnConnectHandler = func(client mqtt.Client) {
	fmt.Println("Connected")
}

var connectionLostHandler mqtt.ConnectionLostHandler = func(client mqtt.Client, err error) {
	fmt.Printf("Connection Lost: %s\n", err.Error())
}

func main() {

	// read config file
	yamlFile, err := ioutil.ReadFile("conf.yaml")
	if err != nil {
		log.Printf("yamlFile.Get err   #%v ", err)
	}

	// convert it to Conf interface
	conf := Conf{}
	err = yaml.Unmarshal(yamlFile, &conf)
	if err != nil {
		log.Fatalf("error: %v", err)
	}
	var stopCondition int
	switch conf.StopCondition {
	case "bytes":
		stopCondition = sizeInBytes
	case "length":
		stopCondition = sizeInLength
	case "time":
		stopCondition = timeInSeconds
	}

	// mqtt utils
	options := mqtt.NewClientOptions()
	options.AddBroker(conf.Broker)
	options.SetClientID("go_mqtt_to_s3")
	options.SetDefaultPublishHandler(messagePubHandler)
	options.OnConnect = connectHandler
	options.OnConnectionLost = connectionLostHandler
	client := mqtt.NewClient(options)
	token := client.Connect()
	if token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	//// s3 utils
	// The session the S3 Uploader will use
	sess, err := session.NewSession(&aws.Config{Region: aws.String("eu-west-3")})
	// Create an uploader with the session and default options
	uploader := s3manager.NewUploader(sess)

	// subscribe the buckets to their channels
	for bucket, topics := range conf.Buckets {
		// for each bucket create a channel
		tmpChannel := make(chan mqtt.Message)
		// the function that will receive all the messages that go to the bucket
		go receiver(bucket, tmpChannel, uploader, stopCondition, conf.MaxSize)
		// the function that is called after mqtt received a message in a channel
		messageHandler := messageHandlerCreator(tmpChannel)
		// subscribe to the channels and set the message handler ass the callbcak
		for _, topic := range topics {
			token = client.Subscribe(topic, 1, messageHandler)
			token.Wait()
			fmt.Printf("Bucket %s is subscribed to topic %s\n", bucket, topic)
		}
	}

	// test code
	num := 5
	for _, topics := range conf.Buckets {
		for _, topic := range topics {
			for i := 0; i < num; i++ {
				text := fmt.Sprintf("hey %d", i)
				token = client.Publish(topic, 0, false, text)
				token.Wait()
				time.Sleep(time.Second)
			}
		}

	}

	client.Disconnect(100)
}
