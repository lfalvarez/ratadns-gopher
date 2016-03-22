package util

//Location struct to marshal the location of QueriesSummary.
//The struct has a longitude, a latitude and the name of the country of that location.
type Location struct {
	Longitude   float64 `json:"longitude"`
	Latitude    float64 `json:"latitude"`
	CountryName string `json:"country_name"`
}
