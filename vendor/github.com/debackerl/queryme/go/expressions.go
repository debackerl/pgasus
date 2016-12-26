package queryme

// A Field is the name of a field.
type Field string

// A Value is any constant used in queries.
type Value interface{}

// A SortOrder is an ordering over a field.
type SortOrder struct {
	Field Field
	Ascending bool
}

// A Predicate is an expression that can is evaluated as either true or false.
type Predicate interface {
	// Accept implements the visitor pattern for predicates.
	Accept(visitor PredicateVisitor)
}

// PredicateVisitor is an object which can visit any kind of predicate.
type PredicateVisitor interface {
	// VisitNot is called to visit a negation predicate.
	VisitNot(operand Predicate)

	// VisitAnd is called to visit a conjunction predicate.
	VisitAnd(operands []Predicate)

	// VisitOr is called to visit a disjunction predicate.
	VisitOr(operands []Predicate)

	// VisitEq is called to visit an equality predicate.
	VisitEq(field Field, operands []Value)

	// VisitLt is called to visit a stricly less than comparison predicate.
	VisitLt(field Field, operand Value)

	// VisitLe is called to visit a less or equal comparison predicate.
	VisitLe(field Field, operand Value)

	// VisitLe is called to visit a stricly greater comparison predicate.
	VisitGt(field Field, operand Value)

	// VisitLe is called to visit a greater or equal comparison predicate.
	VisitGe(field Field, operand Value)

	// VisitLe is called to visit a full-text search predicate.
	VisitFts(field Field, query string)
}

// Not is a negation predicate.
type Not struct {
	Operand Predicate
}

func (p Not) Accept(visitor PredicateVisitor) {
	visitor.VisitNot(p.Operand)
}

// And is a conjunction predicate.
type And []Predicate

func (p And) Accept(visitor PredicateVisitor) {
	visitor.VisitAnd(p)
}

// Or is a disjunction predicate.
type Or []Predicate

func (p Or) Accept(visitor PredicateVisitor) {
	visitor.VisitOr(p)
}

// Eq is an equality predicate.
type Eq struct {
	Field Field
	Operands []Value
}

func (p Eq) Accept(visitor PredicateVisitor) {
	visitor.VisitEq(p.Field, p.Operands)
}

// Lt is a strictly less comparison predicate.
type Lt struct {
	Field Field
	Operand Value
}

func (p Lt) Accept(visitor PredicateVisitor) {
	visitor.VisitLt(p.Field, p.Operand)
}

// Le is a less or equal comparison predicate.
type Le struct {
	Field Field
	Operand Value
}

func (p Le) Accept(visitor PredicateVisitor) {
	visitor.VisitLe(p.Field, p.Operand)
}

// Gt is a stricly greater comparison predicate.
type Gt struct {
	Field Field
	Operand Value
}

func (p Gt) Accept(visitor PredicateVisitor) {
	visitor.VisitGt(p.Field, p.Operand)
}

// Ge is a greater or equal comparison predicate.
type Ge struct {
	Field Field
	Operand Value
}

func (p Ge) Accept(visitor PredicateVisitor) {
	visitor.VisitGe(p.Field, p.Operand)
}

// Ge is a full-text search comparison predicate.
type Fts struct {
	Field Field
	Query string
}

func (p Fts) Accept(visitor PredicateVisitor) {
	visitor.VisitFts(p.Field, p.Query)
}

type fieldsAccumulator struct {
	Index map[Field]struct{}
	Slice []Field
}

func (acc *fieldsAccumulator) saveField(field Field) {
	if _, ok := acc.Index[field]; !ok {
		acc.Index[field] = struct{}{}
		acc.Slice = append(acc.Slice, field)
	}
}

func (acc *fieldsAccumulator) VisitNot(operand Predicate) {
	operand.Accept(acc)
}

func (acc *fieldsAccumulator) VisitAnd(operands []Predicate) {
	for _, p := range operands {
		p.Accept(acc)
	}
}

func (acc *fieldsAccumulator) VisitOr(operands []Predicate) {
	for _, p := range operands {
		p.Accept(acc)
	}
}

func (acc *fieldsAccumulator) VisitEq(field Field, operands []Value) {
	acc.saveField(field)
}

func (acc *fieldsAccumulator) VisitLt(field Field, operand Value) {
	acc.saveField(field)
}

func (acc *fieldsAccumulator) VisitLe(field Field, operand Value) {
	acc.saveField(field)
}

func (acc *fieldsAccumulator) VisitGt(field Field, operand Value) {
	acc.saveField(field)
}

func (acc *fieldsAccumulator) VisitGe(field Field, operand Value) {
	acc.saveField(field)
}

func (acc *fieldsAccumulator) VisitFts(field Field, query string) {
	acc.saveField(field)
}

// Fields returns all fields referenced in the predicate.
func Fields(predicate Predicate) []Field {
	var acc fieldsAccumulator
	acc.Index = make(map[Field]struct{})
	acc.Slice = make([]Field, 0, 4)
	predicate.Accept(&acc)
	return acc.Slice
}
