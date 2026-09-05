package manager

import (
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type msg struct {
	Num int
}
type dataRobot struct {
	R1 string
	R2 string
	R3 string
	R4 string
	R5 string
}
type timeRobot struct {
	timeR1 int64
	timeR2 int64
	timeR3 int64
	timeR4 int64
	timeR5 int64
}

type Execute struct {
	robotExecute   string
	executeTimeout int64
	privilege      int
}

type GlobalBall struct {
	HasBall   bool
	X         int
	Y         int
	Timestamp int64
}

type RobotBallInfo struct {
	HasBall      bool
	X            int
	Y            int
	Distance     float64
	LastSeenTime int64
}

var globalBall = GlobalBall{}
var robotBalls = make(map[string]RobotBallInfo)
var robotBallsMutex sync.Mutex
var execute = Execute{}
var staging = dataRobot{}
var times = timeRobot{}
var gameController = GameController{}
var timeout int64 = 5

func UpdateGlobalBallSelection(now int64) {
	robotBallsMutex.Lock()
	defer robotBallsMutex.Unlock()

	var bestRobot RobotBallInfo
	found := false

	for _, info := range robotBalls {
		if info.HasBall && (now-info.LastSeenTime <= 3) {
			if !found || info.Distance < bestRobot.Distance {
				bestRobot = info
				found = true
			}
		}
	}

	if found {
		globalBall.HasBall = true
		globalBall.X = bestRobot.X
		globalBall.Y = bestRobot.Y
		globalBall.Timestamp = now
	} else {
		globalBall.HasBall = false
	}
}

func Init() {
	gameController.VERSION = 2
	execute.robotExecute = "0"
	execute.privilege = 10 // higher is lower
}

func RefereeBoxHandler() {
	addr := net.UDPAddr{
		Port: 3838,
		IP:   net.ParseIP(GetIP()),
	}
	ser, err := net.ListenUDP("udp", &addr)
	if err != nil {
		fmt.Printf("Some error %v\n", err)
		return
	}

	for {
		bytes := make([]byte, 1024)
		_, _, err := ser.ReadFromUDP(bytes)

		if err != nil {
			fmt.Printf("Some error  %v", err)
			continue
		}
		gameController.STATE = int(bytes[9])
		gameController.KICKOFF = int(bytes[11])
		gameController.SECOND_STATE = int(bytes[12])
		gameController.SECOND_STATE_TEAM = int(bytes[13])
		gameController.SECOND_STATE_CONDITION = int(bytes[14])
	}
}

func ClientHandler() {

	addr := net.UDPAddr{
		Port: 8124,
		IP:   net.ParseIP(GetIP()),
	}

	ser, err := net.ListenUDP("udp", &addr)
	if err != nil {
		fmt.Printf("Some error %v\n", err)
		return
	}

	for {

		bytes := make([]byte, 512)

		n, remoteaddr, err := ser.ReadFromUDP(bytes)
		if err != nil {
			fmt.Printf("[ERROR] ReadFromUDP: %v\n", err)
			continue
		}

		raw := strings.TrimSpace(string(bytes[:n]))
		if raw == "" {
			fmt.Println("[WARN] Received empty data")
			continue
		}

		parts := strings.Split(raw, "|")
		if len(parts) < 3 {
			fmt.Printf("[ERROR] Incomplete encrypted data: %s\n", raw)
			continue
		}

		cipher := parts[0]
		tag := parts[1]
		iv := parts[2]
		plaintext, err := DecryptAESGCM(iv, tag, cipher)

		if err != nil {
			fmt.Printf("[ERROR] Decryption failed: %v\n", err)
			continue
		}

		fmt.Printf("[OK] Decrypted data: %s\n", plaintext)

		received := CleanString(plaintext)
		dataAfterParseLoc := ParseLoc(received)

		fmt.Printf("[INFO] dataAfterParseLoc: %s\n", dataAfterParseLoc)

		s := Split(received)
		if len(s) >= 2 {
			swap := Swap(s[len(s)-1])

			var container string
			for i := 0; i < len(s)-1; i++ {
				container = container + s[i]
			}

			container = CleanString(container)
			swap = CleanString(swap)

			if container == swap {
				fmt.Printf("[OK] Checksum match for robot ID %s\n", GetID(dataAfterParseLoc))

				id := GetID(dataAfterParseLoc)
				t := time.Now()

				// Global Ball Update: Simpan data bola per-robot & pilih bola dari robot berjarak terdekat
				robotIDKey := string(id[0])
				hasBall := (len(s) >= 5 && s[4] == "1")
				if hasBall && len(s) >= 13 {
					bx, _ := strconv.Atoi(s[10])
					by, _ := strconv.Atoi(s[11])
					dist, _ := strconv.ParseFloat(s[12], 64)
					robotBallsMutex.Lock()
					robotBalls[robotIDKey] = RobotBallInfo{
						HasBall:      true,
						X:            bx,
						Y:            by,
						Distance:     dist,
						LastSeenTime: t.Unix(),
					}
					robotBallsMutex.Unlock()
				} else {
					robotBallsMutex.Lock()
					robotBalls[robotIDKey] = RobotBallInfo{
						HasBall:      false,
						LastSeenTime: t.Unix(),
					}
					robotBallsMutex.Unlock()
				}

				UpdateGlobalBallSelection(t.Unix())

				switch id[0] {
				case '1':
					times.timeR1 = t.Unix()
					staging.R1 = dataAfterParseLoc
				case '2':
					times.timeR2 = t.Unix()
					staging.R2 = dataAfterParseLoc
				case '3':
					times.timeR3 = t.Unix()
					staging.R3 = dataAfterParseLoc
				case '4':
					times.timeR4 = t.Unix()
					staging.R4 = dataAfterParseLoc
				case '5':
					times.timeR5 = t.Unix()
					staging.R5 = dataAfterParseLoc
				}

				rvRobot := WhoIsExecute(id)
				go ClientResponse(ser, remoteaddr, rvRobot)
			} else {
				fmt.Printf("[FAIL] Checksum mismatch! container: '%s' vs swap: '%s'\n", container, swap)
			}
		}

	}
}

func ClientResponse(conn *net.UDPConn, addr *net.UDPAddr, rvRobot string) {

	// refereebox
	intRobot, _ := strconv.Atoi(rvRobot)
	data := make([]byte, 12)
	data[0] = byte(gameController.VERSION)
	data[1] = byte(intRobot)
	data[2] = byte(gameController.STATE)
	data[3] = byte(gameController.KICKOFF)
	data[4] = byte(gameController.SECOND_STATE)
	data[5] = byte(gameController.SECOND_STATE_TEAM)
	data[6] = byte(gameController.SECOND_STATE_CONDITION)

	// Global Ball Broadcast
	t := time.Now().Unix()
	if globalBall.HasBall && (t-globalBall.Timestamp <= 3) {
		data[7] = 1
	} else {
		data[7] = 0
	}

	bx := uint16(int16(globalBall.X))
	by := uint16(int16(globalBall.Y))

	data[8] = byte(bx >> 8)
	data[9] = byte(bx & 0xFF)
	data[10] = byte(by >> 8)
	data[11] = byte(by & 0xFF)

	_, err := conn.WriteToUDP(data, addr)
	if err != nil {
		fmt.Printf("Couldn't send response %v", err)
	}
}

func WSHandler(w http.ResponseWriter, r *http.Request) {

	if r.Header.Get("Origin") != "http://"+r.Host {
		http.Error(w, "Origin not allowed", 403)
		return
	}
	conn, err := websocket.Upgrade(w, r, w.Header(), 1024, 1024)
	if err != nil {
		http.Error(w, "Could not open websocket connection", http.StatusBadRequest)
	}

	WSResponse(conn)
}

func WSResponse(conn *websocket.Conn) {
	for {
		m := msg{}

		err := conn.ReadJSON(&m)
		if err != nil {
			fmt.Println("Error reading json.", err)
			break
		}
		var dataResponse string

		t := time.Now()

		if m.Num == 0 {
			dataResponse = "00," //for referee
			dataResponse = dataResponse + strconv.Itoa(gameController.STATE)

		} else if m.Num == 1 {
			var status string

			if times.timeR1+timeout > t.Unix() {
				status = ",on"
			} else {
				status = ",off"
			}

			dataResponse = staging.R1
			dataResponse = dataResponse + status

		} else if m.Num == 2 {
			var status string

			if times.timeR2+timeout > t.Unix() {
				status = ",on"
			} else {
				status = ",off"
			}

			dataResponse = staging.R2
			dataResponse = dataResponse + status

		} else if m.Num == 3 {
			var status string

			if times.timeR3+timeout > t.Unix() {
				status = ",on"
			} else {
				status = ",off"
			}

			dataResponse = staging.R3
			dataResponse = dataResponse + status

		} else if m.Num == 4 {
			var status string

			if times.timeR4+timeout > t.Unix() {
				status = ",on"
			} else {
				status = ",off"
			}

			dataResponse = staging.R4
			dataResponse = dataResponse + status

		} else if m.Num == 5 {
			var status string

			if times.timeR5+timeout > t.Unix() {
				status = ",on"
			} else {
				status = ",off"
			}

			dataResponse = staging.R5
			dataResponse = dataResponse + status
		}

		if err = conn.WriteJSON(dataResponse); err != nil {
			fmt.Println(err)
		}
	}
}

func WhoIsExecute(data string) string {
	if len(data) < 2 {
		return execute.robotExecute
	}

	t := time.Now()

	if execute.executeTimeout+timeout < t.Unix() {
		execute.robotExecute = "0"
		execute.privilege = 10
	}

	if execute.robotExecute != "0" && len(execute.robotExecute) >= 2 {
		if execute.robotExecute[0] == data[0] && data[1] == '0' {
			execute.robotExecute = "0"
			execute.privilege = 10
		}
	}

	if execute.robotExecute == "0" && data[1] == '1' {
		execute.robotExecute = data
		execute.executeTimeout = t.Unix()
	}

	if execute.robotExecute == data {
		execute.executeTimeout = t.Unix()
	}

	return execute.robotExecute
}
