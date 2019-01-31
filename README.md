# SDMX Harvester

## Harvest approach
### Extract
1. Retrieve a Structural Definitions Exchange Message: http://ec.europa.eu/eurostat/SDMX/diss-web/rest/dataflow/ESTAT/all/latest
2. Iterate over all dataflows: select data structure use StructureParser (source see below): We should be able to use an SDMXBean for that.
3. Retrieve the data structure message: http://ec.europa.eu/eurostat/SDMX/diss-web/rest/datastructure/ESTAT/<data structure>
4. Data structure messages are the input for the Transformer (simple XML)

### Transform
0. Use a StructureParser to create a SDMXBean out of the SDMX-ML describing the data structure message.
1. Get all dimensions (keep english description of code); eventually select dimensions as configured.
2. Get all codes for each selected dimension (keep english description of code).
   Let d1, d2,..., dn be the number selected dimensions and dxN the number of codes for dimension dx: We then have d1N x d2N x ... x dnN manifestation of a data structure (Eurostat: 6360)
3. Iterate over each manifestation and create a DataCiteJson object:

| ID | Field                    | Value |
| -: | :----------------------- | :---- |
| 1  | Identifier               | http://ec.europa.eu/eurostat/wdds/rest/data/v2.1/json/en/<data structure id>?d1_name=d1x_name&d2_name=d2y_name...dn_name=d2z_name |
| 2  | Creator                  | - |
| 3  | Title                    | <name of the data structure> (<name of code d1x>, <name of code d2y>, ... <name of code dnz>) |
| 4  | Publisher                | configurable: "Eurostat" |
| 5  | PublicationYear          | (maybe dimension TIME in the future) |
| 6  | Subject                  | Each pair dimension name and code name a subject |
| 7  | Contributor              | - |
| 8  | Date                     | (maybe dimension TIME in the future) |
| 9  | Language                 | configurable: "en" |
| 10 | ResourceType             | Statistical Data (ResourceTypeGeneral |  Dataset) |
| 11 | AlternativeIdentifier    | (maybe urn-scheme in the future) |
| 12 | RelatedIdentifier        | - |
| 13 | Size                     | - |
| 14 | Format                   | configurable: "application/json" |
| 15 | Version                  | - |
| 16 | Rights                   | configurable: "Eurostat License" with rightsUri set to "https | //ec.europa.eu/eurostat/about/policies/copyright" (configurable) |
| 17 | Description              | Automatic compiled text from descriptions of data flows, dimensions and Codelist |
| 18 | GeoLocation              | (maybe CL_GEO in the future) |
| 19 | FundingReference         | - |

Loader
We will use the standard loader for the ES scheme.

## References & Resources
* https://ec.europa.eu/eurostat/web/sdmx-web-services/sdmx (SDMX for eurostat)
* https://sdmx.org/wp-content/uploads/ecb-tutorial.html#id258974  (rough overview over sdmx)
* http://ec.europa.eu/eurostat/SDMX/diss-web/rest/dataflow/ESTAT/all/latest (example  Structural Definitions Exchange Message)
* http://ec.europa.eu/eurostat/SDMX/diss-web/rest/datastructure/ESTAT/DSD_nama_10_gdp (example data structure definition)
* http://www.sdmxsource.org/wp-content/uploads/2013/09/ProgrammersGuide.pdf (good introduction)
* http://www.sdmxsource.org/sdmxsource-java/documentation/using-sdmxsource/ (quickstart)
* http://www.sdmxsource.org/javadoc/1.5.6.2/index.html (JavaDoc)
