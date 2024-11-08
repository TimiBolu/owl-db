package config

func InitServer() {
	ConnectBadgerDB()
}

func TerminateServer() {
	DisconnectBadgerDB()
}
