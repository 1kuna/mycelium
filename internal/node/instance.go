package node

import "fmt"

func (a *Agent) nextInstanceID() string {
	a.nextID++
	return fmt.Sprintf("inst_%d", a.nextID)
}
