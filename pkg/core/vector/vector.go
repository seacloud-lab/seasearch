package vector

// CurIndexVersion
// In the future, the organization format of the data may change,
// and there may be incompatibility with the existing format.
// We identify the current version in the code.
// If the version of the vector data is lower than the current version, it will be processed in compatibility mode.
const CurIndexVersion = 1.0
const CurSegmentVersion = 1.0

const IndexExt = ".index"

// VecPrefix Used to distinguish between vec index and bluge data stored in oss
const VecPrefix = "vec_index"
const (
	None   Action = -1
	Insert Action = 0
	Update Action = 1
	Delete Action = 2
)

const IvfPQ = "ivf_pq"
const Flat = "flat"

const StatusGrowing = "growing"
const StatusSealed = "sealed"

type Action int

// VecAction used for write vector index
type VecAction struct {
	// document Id
	DocId  string
	Vector []float32
	// insert | update | delete
	Action Action
	// zinc index name
	Index string
	Field string
}
