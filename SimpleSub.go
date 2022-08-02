package main

import (
	"bytes"
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
	Broker  string              `yaml:"broker"`
	Buckets map[string][]string `yaml:"buckets"`
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

func receiver(bucket string, channel chan mqtt.Message, uploader *s3manager.Uploader) {
	for msg := range channel {
		fmt.Printf("Message %s received on topic %s and will be uploaded to bucket %s\n",
			msg.Payload(), msg.Topic(), bucket)
		// to phase 1 - upload the message to s3

		result, err := uploader.Upload(&s3manager.UploadInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(msg.Topic() + "/" + time.Now().String()),
			Body:   bytes.NewReader(msg.Payload()),
		})
		if err != nil {
			fmt.Printf("failed to upload file, %v\n", err)
		}
		fmt.Printf("Message %s with topic %s is uploaded to bucket %s\n ID = %s, location = %s",
			msg.Payload(), msg.Topic(), bucket, result.UploadID, result.Location)
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
		go receiver(bucket, tmpChannel, uploader)
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
				text := fmt.Sprintf("%d", i)
				token = client.Publish(topic, 0, false, text)
				token.Wait()
				time.Sleep(time.Second)
			}
		}

	}

	client.Disconnect(100)
}
