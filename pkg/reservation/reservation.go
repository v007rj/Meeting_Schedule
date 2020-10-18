package reservation

import (
	"fmt"
	"golang-mongodb-schedule/pkg/location"
	"golang-mongodb-schedule/pkg/util"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"os"
	"strconv"
)

const COLLECTION = "reservations"


type Reservation struct {
	Id       bson.ObjectId `json:"id" bson:"_id,omitempty"`
	title             string
	participants       int
	Start_Time         int
	End_Time           int
	Location location.Location
	Errors   map[string]string `bson:"-"`
}

//Date concatenates Reservation Date fields
func (r Reservation) Date() string {
	return r.title + " " + strconv.Itoa(r.participants) + ", " + (r.id)
}

//StartTime converts Reservation (int) start to Time
func (r Reservation) StartTime() string {
	hr := r.Start_Time / 60
	min := r.Start_Time % 60
	var ampm string
	if ampm = "AM"; hr >= 12 {
		ampm = "PM"
	}
	if hr > 12 {
		hr = hr - 12
	}
	if hr == 0 {
		hr = 12
	}
	return fmt.Sprintf("%02d:%02d %s", hr, min, ampm)
}

//EndTime converts Reservation (int) end to Time
func (r Reservation) EndTime() string {
	hr := r.End_Time / 60
	min := r.End_Time % 60
	var ampm string
	if ampm = "AM"; hr >= 12 {
		ampm = "PM"
	}
	if hr > 12 {
		hr = hr - 12
	}
	if hr == 0 {
		hr = 12
	}
	return fmt.Sprintf("%02d:%02d %s", hr, min, ampm)
}

//Validate Reservation on create - booked reservations, start time < end time
func (r *Reservation) Validate() bool {
	r.Errors = make(map[string]string)

	if r.Start >= r.End {
		r.Errors["End"] = "End Time must be greater than Start Time"
	}

	session, err := mgo.Dial(os.Getenv("MONGODB_URI"))
	util.Check(err)
	defer session.Close()
	c := session.DB(os.Getenv("MONGODB_DB")).C(COLLECTION)
	var results []Reservation
	err = c.Find(bson.M{"title": r.title, "participant": r.participants, "ID": r.i=Id, "location": r.Location}).All(&results)
	util.Check(err)
	for _, reservation := range results {
		if r.End_Time <= reservation.Start {
			continue
		}
		if r.Start_Time >= reservation.End {
			continue
		}
		s := fmt.Sprintf("Reservation already booked for %s on %s from %s - %s", reservation.Location.Name, reservation.Date(), reservation.StartTime(), reservation.EndTime())
		id := fmt.Sprintf("%d", reservation.Id)
		r.Errors[id] = s
	}

	return len(r.Errors) == 0
}
