package main

import (
	"fmt"
	"time"

	config "github.com/TimiBolu/owl-db/owl-db-config"
	core "github.com/TimiBolu/owl-db/owl-db-core"
)

type Product struct {
	ID        string                 `bson:"_id"`
	Category  string                 `bson:"category"`
	Price     float64                `bson:"price"`
	InStock   bool                   `bson:"inStock"`
	Ratings   map[string]interface{} `bson:"ratings"`
	Tags      []string               `bson:"tags"`
	CreatedAt time.Time              `bson:"createdAt"`
	UpdatedAt time.Time              `bson:"updatedAt"`
}

type Order struct {
	ID           string                   `bson:"_id"`
	CustomerName string                   `bson:"customerName"`
	OrderDate    time.Time                `bson:"orderDate"`
	TotalPrice   float64                  `bson:"totalPrice"`
	Items        []map[string]interface{} `bson:"items"`
	CreatedAt    time.Time                `bson:"createdAt"`
	UpdatedAt    time.Time                `bson:"updatedAt"`
}

// Implement the Document interface
func (p *Product) GetID() string {
	return p.ID
}

func (p *Product) SetID(id string) {
	p.ID = id
}

func (p *Product) SetCreatedAt() {
	p.CreatedAt = time.Now()
}

func (p *Product) SetUpdatedAt() {
	p.UpdatedAt = time.Now()
}

// Implement the Document interface
func (o *Order) GetID() string {
	return o.ID
}

func (o *Order) SetID(id string) {
	o.ID = id
}

func (o *Order) SetCreatedAt() {
	o.CreatedAt = time.Now()
}

func (o *Order) SetUpdatedAt() {
	o.UpdatedAt = time.Now()
}

func main() {
	config.InitServer()

	productCollection := core.NewCollection[*Product](
		config.BadgerDBClient,
		"products",
		core.CollectionOptions{
			Timestamp: true,
		},
	)
	// orderCollection := core.NewCollection[*Order](
	// 	config.BadgerDBClient,
	// 	"orders",
	// )

	// product1 := &Product{
	// 	Category: "Cookware",
	// 	Price:    1200,
	// 	InStock:  true,
	// 	Ratings:  map[string]interface{}{"score": 4.5},
	// 	Tags:     []string{"gaming", "laptop"},
	// }

	// err := productCollection.Insert(product1)
	// if err != nil {
	// 	fmt.Println(err)
	// }

	// productList := []*Product{}

	// var count int
	// for {
	// 	count++

	// 	productList = append(productList, &Product{
	// 		Category: "A",
	// 		Price:    float64(1200 + count),
	// 		InStock:  true,
	// 		Ratings:  map[string]interface{}{"score": 4.5},
	// 		Tags:     []string{"gaming", "laptop"},
	// 	})

	// 	if count == 999 {
	// 		break
	// 	}
	// }

	// err := productCollection.InsertMany(productList)
	// if err != nil {
	// 	fmt.Println(err)
	// }

	// err := productCollection.UpdateMany(
	// 	core.Filter{
	// 		"price": 2137.0,
	// 	},
	// 	core.Update{
	// 		"$set": map[string]interface{}{
	// 			"inStock":       false,
	// 			"ratings.score": 4.2,
	// 		},
	// 	},
	// )

	// if err != nil {
	// 	fmt.Println(err)
	// }

	// productCollection.DeleteMany(
	// 	core.Filter{
	// 		"ratings.score": 4.5,
	// 	},
	// )

	// productCollection.UpdateByID("6707933f58d90649c2cc7e9d",
	// 	core.Update{
	// 		"$set": map[string]interface{}{
	// 			"category": "A",
	// 		},
	// 	},
	// )

	// time.Sleep(time.Second * 3)

	// productCollection.UpdateByID("670793f3d73d71be4b85e2e2",
	// 	core.Update{
	// 		"$set": map[string]interface{}{
	// 			"category": "B",
	// 		},
	// 	},
	// )

	// time.Sleep(time.Second * 3)

	// productCollection.UpdateByID("67076eeeb6cd4c8b5178c119",
	// 	core.Update{
	// 		"$set": map[string]interface{}{
	// 			"category": "C",
	// 		},
	// 	},
	// )

	// time.Sleep(time.Second * 3)

	// productCollection.UpdateByID("670793fa305e60f18ec10019",
	// 	core.Update{
	// 		"$set": map[string]interface{}{
	// 			"category": "D",
	// 		},
	// 	},
	// )

	// prod, err := productCollection.FindByID("6707933f58d90649c2cc7e9d")
	// fmt.Println(prod, " -- ", err)

	products, err := productCollection.Find(
		core.Filter{
			"_id": "670793fa305e60f18ec0fff4",
		},
		core.FindOptions{
			Skip:  1,
			Limit: 2,
			Select: map[string]bool{
				"price":         false,
				"ratings.score": false,
			},
			Sort: []core.SortField{
				{
					Field: "ratings.score",
					Order: 1,
				},
			},
		},
	)
	if err != nil {
		fmt.Println(err)
	}

	// fmt.Println(product)

	for _, product := range products {
		fmt.Println(product)
	}
	fmt.Println(len(products))

	// order1 := &Order{
	// 	CustomerName: "Pat Doe",
	// 	OrderDate:    time.Now(),
	// 	TotalPrice:   1200,
	// 	Items: []map[string]interface{}{
	// 		{"productID": "1", "quantity": 1},
	// 	},
	// }
	// err = orderCollection.Insert(order1)
	// if err != nil {
	// 	fmt.Println(err)
	// }

	// productCollection.Update(
	// 	"67076eeeb6cd4c8b5178c119",
	// 	core.Update{
	// 		"$set": map[string]interface{}{
	// 			"price":         250.0,
	// 			"ratings.score": 4.3,
	// 		},
	// 	},
	// )

	// orderCollection.Update(
	// 	"67076eeeb6cd4c8b5178c11a",
	// 	core.Update{
	// 		"$set": map[string]interface{}{
	// 			"totalPrice":   560.0,
	// 			"customerName": "John Smith",
	// 		},
	// 	},
	// )

	// product, err := productCollection.FindByID("67076eeeb6cd4c8b5178c119")
	// if err != nil {
	// 	fmt.Println(err)
	// }
	// fmt.Println(product)

	// order, err := orderCollection.FindByID("67076eeeb6cd4c8b5178c11a")
	// if err != nil {
	// 	fmt.Println(err)
	// }
	// fmt.Println(order)

	defer config.TerminateServer()
}

type Person struct {
	Gender   string `bson:"gender"`
	Employer string `bson:"employer" ref:"employers"` // employer's id
}

type Employer struct {
	Name     string `bson:"name"`
	Industry string `bson:"industry" ref:"industries"` // industry id
	Country  string `bson:"country" ref:"countries"`   // country id
}

type Industry struct {
	Name string `bson:"name"`
}

type Continent string

const Africa Continent = "africa"
const Asia Continent = "asia"
const Europe Continent = "europe"

type Country struct {
	Name      string    `bson:"name"`
	Continent Continent `bson:"continent"`
}

// PersonCollection.Find(
// 	core.Filter{
// 		"gender": "male",
// 		"employer.name": core.Filter{"$startsWith": "Tesla"},
// 		"employer.industry.name": "Technology",
// 		"employer.country.location": {
// 			"$near": core.GeoPoint{Lat: 40.7128, Lon: -74.0060},
// 		},
// 	},
// 	core.FindOptions{
// 		Skip:  1,
// 		Limit: 2,
// 		Select: map[string]bool{
// 			"employer.country.continent": true,
// 			"gender": false,
// 		},
// 		Sort: []core.SortField{
// 			{
// 				Field: "employer.country.location",
// 				Order: 1,
// 			},
// 		},
// 	},
// );
