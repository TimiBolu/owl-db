package core

// Document is an interface that requires any type to have an ID field.
type Document interface {
	GetID() string
	SetID(id string)
	SetCreatedAt()
	SetUpdatedAt()
}
