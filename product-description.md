# COVIDcast (COVID-19) Epidemiological Data | Delphi Research Group (CMU)

## Main Description
This resource contains an archived collection of datasets from Carnegie Mellon University Delphi Research Group's COVID-19 Surveillance Streams Data (COVIDcast) - itself an endpoint in Delphi's Epidata open API for Epidemiological Data.

Delphi's COVIDcast datasets are based on a variety of data sources including **a CMU-run Facebook health survey, a Google-run health survey, lab test results provided by Quidel Inc, search data released by Google Health Trends, and outpatient doctor visits provided by a national health system**.

## Schema
The datasets in this product are delivered with descriptive S3 prefixes set to mirror the same parameters used when interacting directly with the COVIDcast API. A `file_format` parameter has been added as a base prefix to be able to easily navigate between the JSON Lines and CSV versions of this data.

`/<file_format>/<data_source>/< signal >/<time_type>/<geo_type>.<file_extension>`

For example, to access a JSON Lines file covering Facebook survey data for COVID-Like Illnesses (CLI) broken into daily entries by two-letter state codes, you would visit the file at the following path:

`/jsonl/fb-survey/raw_cli/day/state.jsonl`

Individual data entries include the following fields:

`geo_value` | `time_value` | `direction` | `value` | `stderr` | `sample_size`

To learn about the valid parameters and fields used in the Delphi's COVIDcast data, please visit the [Delphi Epidata GitHub repository](https://github.com/cmu-delphi/delphi-epidata/blob/master/docs/api/covidcast.md) where this project is actively maintained.

## More Information:
- [Source: Delphi Epidata | An open API for Epidemiological Data](https://github.com/cmu-delphi/delphi-epidata)
- [Home Page: Delphi Research Group | Carnegie Mellon University](https://delphi.cmu.edu)
- [COVIDcast | Carnegie Mellon University](https://covidcast.cmu.edu)
- [Delphi EpiData | Data License](https://github.com/cmu-delphi/delphi-epidata/blob/master/docs/api/README.md#data-licensing)
- Frequency: Daily
- Formats: JSON Lines, CSV

## Contact/Support Information
- If you find any issues or have enhancements with this product, open up a GitHub [issue](https://github.com/rearc-data/covid-datasets-aws-data-exchange/issues/new) and we will gladly take a look at it. Better yet, submit a pull request. Any contributions you make are greatly appreciated.
- If you are interested in any other open datasets, please create a request on our project board [here](https://github.com/rearc-data/covid-datasets-aws-data-exchange/projects/1).
- If you have questions about this source data, please contact a memeber of the Delphi Research Group by [opening an issue on this project's GitHub page](https://github.com/cmu-delphi/delphi-epidata/issues/new).
- If you have any other questions or feedback, send us an email at data@rearc.io.

## About Rearc
Rearc is a cloud, software and services company. We believe that empowering engineers drives innovation. Cloud-native architectures, modern software and data practices, and the ability to safely experiment can enable engineers to realize their full potential. We have partnered with several enterprises and startups to help them achieve agility. Our approach is simple — empower engineers with the best tools possible to make an impact within their industry.