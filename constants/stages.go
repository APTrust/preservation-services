package constants

type Stage struct {
	Action   string
	Name     string
	Order    int64
	NSQTopic string
}

var IngestStages = []Stage{
	{
		Action:   ActionIngest,
		Name:     StageReceive,
		Order:    1,
		NSQTopic: IngestPreFetch,
	},
	{
		Action:   ActionIngest,
		Name:     StageValidate,
		Order:    2,
		NSQTopic: IngestValidation,
	},
	{
		Action:   ActionIngest,
		Name:     StageReingestCheck,
		Order:    3,
		NSQTopic: IngestReingestCheck,
	},
	{
		Action:   ActionIngest,
		Name:     StageCopyToStaging,
		Order:    4,
		NSQTopic: IngestStaging,
	},
	{
		Action:   ActionIngest,
		Name:     StageFormatIdentification,
		Order:    5,
		NSQTopic: IngestFormatIdentification,
	},
	{
		Action:   ActionIngest,
		Name:     StageStore,
		Order:    6,
		NSQTopic: IngestStorage,
	},
	{
		Action:   ActionIngest,
		Name:     StageStorageValidation,
		Order:    7,
		NSQTopic: IngestStorageValidation,
	},
	{
		Action:   ActionIngest,
		Name:     StageRecord,
		Order:    8,
		NSQTopic: IngestRecord,
	},
	{
		Action:   ActionIngest,
		Name:     StageCleanup,
		Order:    9,
		NSQTopic: IngestCleanup,
	},
}
