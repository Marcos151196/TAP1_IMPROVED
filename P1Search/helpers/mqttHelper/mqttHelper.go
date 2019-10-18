package mqttHelper

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	log "github.com/sirupsen/logrus"
	viper "github.com/theherk/viper"
)

type mqttInfo struct {
	Broker    string
	User      string
	Password  string
	Timeout   int
	Keepalive int
}

var MQTTInfo mqttInfo
var mqttC mqtt.Client

func (info *mqttInfo) getMqttInfo() {
	info.Broker = viper.GetString("mqtt.broker")
	info.User = viper.GetString("mqtt.username")
	info.Password = viper.GetString("mqtt.password")
	info.Timeout = viper.GetInt("mqtt.timeout")
	info.Keepalive = viper.GetInt("mqtt.keepalive")
}

func OpenMQTT(onConnect func(), onDisconnect func()) {
	MQTTInfo.getMqttInfo()
	sleepTime := 1
	go func() {
		for {
			err := launchMQTT(MQTTInfo.Broker, MQTTInfo.User, MQTTInfo.Password, MQTTInfo.Keepalive, MQTTInfo.Timeout,
				func(c mqtt.Client) {
					mqttC = c
					onConnect()
				},
				func(c mqtt.Client, err error) {
					mqttC = nil
					onDisconnect()
				})
			if err == nil {
				break
			} else {
				sleepTime = sleepTime * 2
				if sleepTime > 15 {
					sleepTime = 1
				}
				log.Errorf("%s. Sleeping %d seconds and reconnect", err.Error(), sleepTime)
				time.Sleep(time.Duration(sleepTime) * time.Second)
			}
		}
	}()
}

func launchMQTT(mqttBroker string, username string, password string, mKeepAlive int, mTimeout int, onConnect func(mqtt.Client), onDisconnect func(mqtt.Client, error)) error {
	log.Infof("[MQTT] Connecting to MQTT Broker: %s", mqttBroker)

	err := checkConnection(mqttBroker)
	if err != nil {
		return err
	}

	opts := mqtt.NewClientOptions()
	opts.AddBroker(mqttBroker)
	opts.SetUsername(username)
	opts.SetPassword(password)
	opts.SetAutoReconnect(true)
	opts.SetKeepAlive(time.Duration(mKeepAlive) * time.Second)
	opts.SetPingTimeout(time.Duration(mTimeout) * time.Second)
	opts.SetOnConnectHandler(onConnect)
	opts.SetConnectionLostHandler(onDisconnect)

	mqttClient := mqtt.NewClient(opts)
	if token := mqttClient.Connect(); token.Wait() && token.Error() != nil {
		return errors.New(fmt.Sprintf("[MQTT] MQTT Connect Error %v", token.Error()))
	}

	return nil
}

func checkConnection(mqttBroker string) error {
	u, err := url.Parse(mqttBroker)
	if err != nil {
		return errors.New(fmt.Sprintf("Unable to parse URL from MQTT Broker (%s): %s", mqttBroker, err.Error()))
	}

	conn, err := net.DialTimeout(u.Scheme, u.Host, 2*time.Second)
	if conn != nil {
		conn.Close()
	}
	if err != nil {
		return errors.New(fmt.Sprintf("Unable to connect (dial) to MQTT Broker (%s://%s): %s", u.Scheme, u.Host, err.Error()))
	}
	return nil
}

func Subscribe(topic string, qos byte, callback func(string, []byte)) error {
	token := mqttC.Subscribe(topic, byte(0),
		func(c mqtt.Client, msg mqtt.Message) {
			callback(msg.Topic(), msg.Payload())
		})
	if token.Wait() && token.Error() != nil {
		return token.Error()
	}
	return nil
}

func Publish(topic string, msg string) error {
	if IsConnected() {
		token := mqttC.Publish(topic, 0, true, []byte(msg))
		token.Wait()
		if token.Error() != nil {
			return token.Error()
		}

	} else {
		return fmt.Errorf("COM IsConnected returned FALSE")
	}

	return nil
}

func IsConnected() bool {
	if mqttC != nil {
		if mqttC.IsConnected() {
			return true
		}
	} else {
		return false
	}
	return false
}

func Disconnect() error {
	if IsConnected() {
		mqttC.Disconnect(500)
	} else {
		return errors.New("COM connection was already down, so failed when trying to Disconnect")
	}
	return nil
}
