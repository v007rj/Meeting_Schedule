package mgo

import (
	"bytes"
	"sort"

	"gopkg.in/mgo.v2/bson"
)

//
type Bulk struct {
	c       *Collection
	opcount int
	actions []bulkAction
	ordered bool
}

type bulkOp int

const (
	bulkInsert bulkOp = iota + 1
	bulkUpdate
	bulkUpdateAll
	bulkRemove
)

type bulkAction struct {
	op   bulkOp
	docs []interface{}
	idxs []int
}

type bulkUpdateOp []interface{}
type bulkDeleteOp []interface{}


type BulkResult struct {
	Matched  int
	Modified int 

	private bool
}


type BulkError struct {
	ecases []BulkErrorCase
}

func (e *BulkError) Error() string {
	if len(e.ecases) == 0 {
		return "invalid BulkError instance: no errors"
	}
	if len(e.ecases) == 1 {
		return e.ecases[0].Err.Error()
	}
	msgs := make([]string, 0, len(e.ecases))
	seen := make(map[string]bool)
	for _, ecase := range e.ecases {
		msg := ecase.Err.Error()
		if !seen[msg] {
			seen[msg] = true
			msgs = append(msgs, msg)
		}
	}
	if len(msgs) == 1 {
		return msgs[0]
	}
	var buf bytes.Buffer
	buf.WriteString("multiple errors in bulk operation:\n")
	for _, msg := range msgs {
		buf.WriteString("  - ")
		buf.WriteString(msg)
		buf.WriteByte('\n')
	}
	return buf.String()
}

type bulkErrorCases []BulkErrorCase

func (slice bulkErrorCases) Len() int           { return len(slice) }
func (slice bulkErrorCases) Less(i, j int) bool { return slice[i].Index < slice[j].Index }
func (slice bulkErrorCases) Swap(i, j int)      { slice[i], slice[j] = slice[j], slice[i] }


type BulkErrorCase struct {
	Index int 
	Err   error
}

func (e *BulkError) Cases() []BulkErrorCase {
	return e.ecases
}


func (c *Collection) Bulk() *Bulk {
	return &Bulk{c: c, ordered: true}
}


func (b *Bulk) Unordered() {
	b.ordered = false
}

func (b *Bulk) action(op bulkOp, opcount int) *bulkAction {
	var action *bulkAction
	if len(b.actions) > 0 && b.actions[len(b.actions)-1].op == op {
		action = &b.actions[len(b.actions)-1]
	} else if !b.ordered {
		for i := range b.actions {
			if b.actions[i].op == op {
				action = &b.actions[i]
				break
			}
		}
	}
	if action == nil {
		b.actions = append(b.actions, bulkAction{op: op})
		action = &b.actions[len(b.actions)-1]
	}
	for i := 0; i < opcount; i++ {
		action.idxs = append(action.idxs, b.opcount)
		b.opcount++
	}
	return action
}


func (b *Bulk) Insert(docs ...interface{}) {
	action := b.action(bulkInsert, len(docs))
	action.docs = append(action.docs, docs...)
}


func (b *Bulk) Remove(selectors ...interface{}) {
	action := b.action(bulkRemove, len(selectors))
	for _, selector := range selectors {
		if selector == nil {
			selector = bson.D{}
		}
		action.docs = append(action.docs, &deleteOp{
			Collection: b.c.FullName,
			Selector:   selector,
			Flags:      1,
			Limit:      1,
		})
	}
}

func (b *Bulk) RemoveAll(selectors ...interface{}) {
	action := b.action(bulkRemove, len(selectors))
	for _, selector := range selectors {
		if selector == nil {
			selector = bson.D{}
		}
		action.docs = append(action.docs, &deleteOp{
			Collection: b.c.FullName,
			Selector:   selector,
			Flags:      0,
			Limit:      0,
		})
	}
}


func (b *Bulk) Update(pairs ...interface{}) {
	if len(pairs)%2 != 0 {
		panic("Bulk.Update needs to have an even number of parameters")
	}
	action := b.action(bulkUpdate, len(pairs)/2)
	for i := 0; i < len(pairs); i += 2 {
		selector := pairs[i]
		if selector == nil {
			selector = bson.D{}
		}
		action.docs = append(action.docs, &updateOp{
			Collection: b.c.FullName,
			Selector:   selector,
			Update:     pairs[i+1],
		})
	}
}


func (b *Bulk) UpdateAll(pairs ...interface{}) {
	if len(pairs)%2 != 0 {
		panic("Bulk.UpdateAll needs to have an even number of parameters")
	}
	action := b.action(bulkUpdate, len(pairs)/2)
	for i := 0; i < len(pairs); i += 2 {
		selector := pairs[i]
		if selector == nil {
			selector = bson.D{}
		}
		action.docs = append(action.docs, &updateOp{
			Collection: b.c.FullName,
			Selector:   selector,
			Update:     pairs[i+1],
			Flags:      2,
			Multi:      true,
		})
	}
}


func (b *Bulk) Upsert(pairs ...interface{}) {
	if len(pairs)%2 != 0 {
		panic("Bulk.Update needs to have an even number of parameters")
	}
	action := b.action(bulkUpdate, len(pairs)/2)
	for i := 0; i < len(pairs); i += 2 {
		selector := pairs[i]
		if selector == nil {
			selector = bson.D{}
		}
		action.docs = append(action.docs, &updateOp{
			Collection: b.c.FullName,
			Selector:   selector,
			Update:     pairs[i+1],
			Flags:      1,
			Upsert:     true,
		})
	}
}


func (b *Bulk) Run() (*BulkResult, error) {
	var result BulkResult
	var berr BulkError
	var failed bool
	for i := range b.actions {
		action := &b.actions[i]
		var ok bool
		switch action.op {
		case bulkInsert:
			ok = b.runInsert(action, &result, &berr)
		case bulkUpdate:
			ok = b.runUpdate(action, &result, &berr)
		case bulkRemove:
			ok = b.runRemove(action, &result, &berr)
		default:
			panic("unknown bulk operation")
		}
		if !ok {
			failed = true
			if b.ordered {
				break
			}
		}
	}
	if failed {
		sort.Sort(bulkErrorCases(berr.ecases))
		return nil, &berr
	}
	return &result, nil
}

func (b *Bulk) runInsert(action *bulkAction, result *BulkResult, berr *BulkError) bool {
	op := &insertOp{b.c.FullName, action.docs, 0}
	if !b.ordered {
		op.flags = 1 // ContinueOnError
	}
	lerr, err := b.c.writeOp(op, b.ordered)
	return b.checkSuccess(action, berr, lerr, err)
}

func (b *Bulk) runUpdate(action *bulkAction, result *BulkResult, berr *BulkError) bool {
	lerr, err := b.c.writeOp(bulkUpdateOp(action.docs), b.ordered)
	if lerr != nil {
		result.Matched += lerr.N
		result.Modified += lerr.modified
	}
	return b.checkSuccess(action, berr, lerr, err)
}

func (b *Bulk) runRemove(action *bulkAction, result *BulkResult, berr *BulkError) bool {
	lerr, err := b.c.writeOp(bulkDeleteOp(action.docs), b.ordered)
	if lerr != nil {
		result.Matched += lerr.N
		result.Modified += lerr.modified
	}
	return b.checkSuccess(action, berr, lerr, err)
}

func (b *Bulk) checkSuccess(action *bulkAction, berr *BulkError, lerr *LastError, err error) bool {
	if lerr != nil && len(lerr.ecases) > 0 {
		for i := 0; i < len(lerr.ecases); i++ {
	
			ecase := lerr.ecases[i]
			idx := ecase.Index
			if idx >= 0 {
				idx = action.idxs[idx]
			}
			berr.ecases = append(berr.ecases, BulkErrorCase{idx, ecase.Err})
		}
		return false
	} else if err != nil {
		for i := 0; i < len(action.idxs); i++ {
			berr.ecases = append(berr.ecases, BulkErrorCase{action.idxs[i], err})
		}
		return false
	}
	return true
}
