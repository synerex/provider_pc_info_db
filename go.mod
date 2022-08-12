module pc_info_db

go 1.15

require (
	github.com/UCLabNU/proto_pflow v0.0.5
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/golang/protobuf v1.5.2
	github.com/kr/text v0.2.0 // indirect
	github.com/niemeyer/pretty v0.0.0-20200227124842-a10e7caefd8e // indirect
	github.com/synerex/proto_pcounter v0.0.6
	github.com/synerex/synerex_api v0.4.3
	github.com/synerex/synerex_proto v0.1.12
	github.com/synerex/synerex_sxutil v0.7.0
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1 // indirect
	google.golang.org/protobuf v1.27.1
	gopkg.in/check.v1 v1.0.0-20200902074654-038fdea0a05b // indirect
	gopkg.in/yaml.v3 v3.0.0-20200615113413-eeeca48fe776 // indirect
)

replace github.com/synerex/proto_pcounter v0.0.6 => github.com/nagata-yoshiteru/proto_pcounter v0.0.10

replace github.com/synerex/synerex_proto v0.1.9 => github.com/nagata-yoshiteru/synerex_proto v0.1.16

