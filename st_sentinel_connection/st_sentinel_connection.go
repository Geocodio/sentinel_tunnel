package st_sentinel_connection

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/DivPro/sentinel_tunnel/st_logger"
	"net"
	"strconv"
	"time"
)

type Get_master_addr_reply struct {
	reply string
	err   error
}

type Sentinel_connection struct {
	sentinels_addresses              []string
	current_sentinel_connection      net.Conn
	reader                           *bufio.Reader
	writer                           *bufio.Writer
	get_master_address_by_name_reply chan *Get_master_addr_reply
	get_master_address_by_name       chan string
}

const (
	client_closed     = true
	client_not_closed = false
)

func (c *Sentinel_connection) parseResponse() (request []string, err error, is_client_closed bool) {
	var ret []string
	buf, _, e := c.reader.ReadLine()
	if e != nil {
		return nil, errors.New("failed read line from client"), client_closed
	}
	if len(buf) == 0 {
		return nil, errors.New("failed read line from client"), client_closed
	}
	if buf[0] != '*' {
		return nil, errors.New("first char in mbulk is not *"), client_not_closed
	}
	mbulk_size, _ := strconv.Atoi(string(buf[1:]))
	if mbulk_size == -1 {
		return nil, errors.New("null request"), client_not_closed
	}
	ret = make([]string, mbulk_size)
	for i := 0; i < mbulk_size; i++ {
		buf1, _, e1 := c.reader.ReadLine()
		if e1 != nil {
			return nil, errors.New("failed read line from client"), client_closed
		}
		if len(buf1) == 0 {
			return nil, errors.New("failed read line from client"), client_closed
		}
		if buf1[0] != '$' {
			return nil, errors.New("first char in bulk is not $"), client_not_closed
		}
		bulk_size, _ := strconv.Atoi(string(buf1[1:]))
		buf2, _, e2 := c.reader.ReadLine()
		if e2 != nil {
			return nil, errors.New("failed read line from client"), client_closed
		}
		bulk := string(buf2)
		if len(bulk) != bulk_size {
			return nil, errors.New("wrong bulk size"), client_not_closed
		}
		ret[i] = bulk
	}
	return ret, nil, client_not_closed
}

func (c *Sentinel_connection) getMasterAddrByNameFromSentinel(db_name string) (addr []string, returned_err error, is_client_closed bool) {
	if c.writer == nil {
		return nil, errors.New("all sentinels failed"), true
	}
	strs := []string{
		"*3\r\n",
		"$8\r\n",
		"sentinel\r\n",
		"$23\r\n",
		"get-master-addr-by-name\r\n",
		fmt.Sprintf("$%d\r\n", len(db_name)),
		db_name,
		"\r\n",
	}
	for _, s := range strs {
		cnt, err := c.writer.WriteString(s)
		if err != nil {
			st_logger.WriteLogMessage(st_logger.ERROR, "write to sentinel error", err.Error())
			return nil, err, true
		}
		st_logger.WriteLogMessage(st_logger.DEBUG, "write to sentinel success", strconv.Itoa(cnt))
	}
	err := c.writer.Flush()
	if err != nil {
		st_logger.WriteLogMessage(st_logger.ERROR, "flush sentinel writer error", err.Error())

		return nil, err, true
	}

	return c.parseResponse()
}

func (c *Sentinel_connection) retrieveAddressByDbName() {
	for db_name := range c.get_master_address_by_name {
		addr, err, is_client_closed := c.getMasterAddrByNameFromSentinel(db_name)
		if err != nil {
			st_logger.WriteLogMessage(st_logger.ERROR, "error while receiving name from sentinel:", err.Error())
			if !is_client_closed {
				c.get_master_address_by_name_reply <- &Get_master_addr_reply{
					reply: "",
					err:   errors.New("failed to retrieve db name from the sentinel, db_name:" + db_name),
				}
			}
			if !c.reconnectToSentinel() {
				c.get_master_address_by_name_reply <- &Get_master_addr_reply{
					reply: "",
					err:   errors.New("failed to connect to any of the sentinel services"),
				}
			} else {
				st_logger.WriteLogMessage(st_logger.INFO, "reconnected successfully")
				go c.retrieveAddressByDbName()
				c.GetAddressByDbName(db_name)
			}
			continue
		}
		c.get_master_address_by_name_reply <- &Get_master_addr_reply{
			reply: net.JoinHostPort(addr[0], addr[1]),
			err:   nil,
		}
	}
}

func (c *Sentinel_connection) reconnectToSentinel() bool {
	for _, sentinelAddr := range c.sentinels_addresses {
		if c.current_sentinel_connection != nil {
			c.current_sentinel_connection.Close()
			c.reader = nil
			c.writer = nil
			c.current_sentinel_connection = nil
		}

		var err error
		c.current_sentinel_connection, err = net.DialTimeout("tcp", sentinelAddr, 300*time.Millisecond)
		if err == nil {
			c.reader = bufio.NewReader(c.current_sentinel_connection)
			c.writer = bufio.NewWriter(c.current_sentinel_connection)

			st_logger.WriteLogMessage(st_logger.DEBUG, "connected to sentinel:", sentinelAddr)

			return true
		}
		st_logger.WriteLogMessage(st_logger.ERROR, "error reconnect to sentinel:", err.Error())
	}
	return false
}

func (c *Sentinel_connection) GetAddressByDbName(name string) (string, error) {
	c.get_master_address_by_name <- name
	reply := <-c.get_master_address_by_name_reply
	return reply.reply, reply.err
}

func NewSentinelConnection(addresses []string) (*Sentinel_connection, error) {
	connection := Sentinel_connection{
		sentinels_addresses:              addresses,
		get_master_address_by_name:       make(chan string),
		get_master_address_by_name_reply: make(chan *Get_master_addr_reply),
		current_sentinel_connection:      nil,
		reader:                           nil,
		writer:                           nil,
	}

	if !connection.reconnectToSentinel() {
		return nil, errors.New("could not connect to any sentinels")
	}

	go connection.retrieveAddressByDbName()

	return &connection, nil
}
