package txn

import (
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"stupid-kv/base"
	log "stupid-kv/logutil"
)

func (m *Manager) FlushTid() {
	//m.flushGuard.Lock()
	//defer m.flushGuard.Unlock()
	f, err := os.Create("STATE.txt") // creating...
	if err != nil {
		log.Error("error create STATE.txt")
		return
	}
	defer f.Close()

	_, err = f.WriteString(fmt.Sprintf("%d\n", m.curTid))
	if err != nil {
		log.Error("error write STATE.txt")
	}

	for i := 0; i < len(m.curActiveTids); i++ { // Generating...
		_, err = f.WriteString(fmt.Sprintf("%d ", m.curActiveTids[i])) // writing...
		if err != nil {
			fmt.Printf("error writing string: %v", err)
		}
	}

}

func (m *Manager) Load() {
	b, err := ioutil.ReadFile("STATE.txt")
	if err != nil {
		log.Warning("no STATE.txt")
		return
	}

	lines := strings.Split(string(b), "\n")
	n, err := strconv.Atoi(lines[0])
	m.curTid = base.Tid(n)

	if len(lines) == 2 {
		//tmp := strings.Trim(lines[1], " ")
		//activeTids := strings.Fields(tmp)
		// TODO: Undo log
	}
	//for _, l := range lines {
	//	// Empty line occurs at the end of the file when we use Split.
	//	if len(l) == 0 { continue }
	//	// Atoi better suits the job when we know exactly what we're dealing
	//	// with. Scanf is the more general option.
	//	n, err := strconv.Atoi(l)
	//	if err != nil { return nil, err }
	//	nums = append(nums, n)
	//}
	//
	//return nums, nil
}

