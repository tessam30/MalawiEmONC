/*-------------------------------------------------------------------------------
# Name:		01_DataProcessing.do
# Purpose:	Create series of folders For Malawi EmONC Assessment Analysis
# Author:	Tim Essam, Ph.D.
# Created:	08/19/2014
# Owner:	USAID GeoCenter | OakStream Systems, LLC
# License:	MIT License
# Ado(s):	see below
#-------------------------------------------------------------------------------
*/

clear
capture log close
log using "$pathlog/01_DataProcessing", replace
set more off
* Import the final assessment data with GPS coordinates
import excel "$pathin\EmOC_work_250714.xlsx", sheet("Final") firstrow clear

* Check unique id, clone it and rename it, and save data
isid uniq_id_Assess
clonevar uniq_id = uniq_id_Assess

mdesc
save "$pathout/final_assessment.dta", replace

* Load assessment survey data and merge with GPS 
use "$pathin/modules1_5Lilongwe.dta", clear
merge 1:1 uniq_id using "$pathout/final_assessment.dta", gen(_merge1)

* Create a district code that is useable
cap do "$pathdo/labvalsort2.do"
decode district, gen(dist)
replace dist = proper(dist)
g byte test_dist = (dist == District_Assess)
tab test_dist
clist distname Name_facility_Assess Facility_type_Assess dist District_Assess if test_dist==0, noo

* Fix QUEC
replace fac_name = proper(fac_name)
replace fac_name = "Queen Elizabeth Central Hospital" if fac_name=="Qech"

/* NOTES: Use the District_Assess variable as the master district variable */

/* Survey Notes: 309 facilities surveyed; Census of all hospitals and 50% of all health centres providing
deliveries. 92 Hospitals included in survey (census); 217 health centers --> 309 *

Weights: In district X, the total number of deliveries is the sum of the total number of deliveries
in hospitals PLUS the actual number of deliveries in sampled health centers and extrapolated numbers
of deliveries in non-sampled health centres.  If there are 5 sampled and 8 non-sampled health centers,
the mean number of deliveries of the 5 sampled health centers is multiplied by the number of non-sampled
health centers (8) and added to the total deliveries found in the 5 sampled health centers to get the 
total number of deliveries in ALL health centers in district X.  
*/

 g byte hCentre = (two_grou==2)

* Merge in district weights derived from excel file (Malawi EmONC Report_Indicator Adjustments (Final))
merge m:1 District_Assess using "U:\MalawiEmONC\Datain\weights.dta"

* Create a geographic grouping based on regions
g region = "."
replace region = "North" if regexm(District_Assess, "(Chitipa|Karonga|Nkhata Bay|Rumphi|Mzimba|Likoma)")==1
replace region = "Central" if regexm(District_Assess, "(Kasungu|Nkhotakota|Ntchisi|Dowa|Salima|Lilongwe|Mchinji|Dedza|Ntcheu)")==1
replace region = "South" if regexm(District_Assess, "(Mangochi|Machinga|Zomba|Chiradzulu|Blantyre|Mwanza|Thyolo|Mulanje|Chikwawa|Nsanje|Phalombe|Balaka|Neno)")==1

* EmONC Status - Check facility categorization and verify
/* What clinics are classified as Emergency Obstetric and Neonatal Care (EmONC)?
	emoncst - international status
	emonc_st - malawi status
	bemonc => basic status
	cemonc => comprehensive status 
	*/
tab3way emonc_st emoncfac emoncsta, rowtot coltot
tab District_Assess emonc_st, mi

/*  Verify EmONC status based on signal functions. Use 3 month window for whether 
	or not a signal function was performed at a facility.  All signal functions
	captured in Module 5 questions 4, 10, 15, 18, 23, 28, 31, 35;
	Signal Functions: 7 signal functions required for Basic status;
					  9 signal functions required for Comprehensive status;
*/
local sigf mod5q1 mod5q4 mod5q10 mod5q15 mod5q18 mod5q23 mod5q28 mod5q31 mod5q35
local i = 1
foreach x of local sigf {
	g byte sf`i' = inlist(`x', 1)
	copydesc `x' sf`i'
	local i = `i' + 1
	}
*end	

* Create a binary variable indicating whether or not facility is BEmONC
* If they sum to 7 == BASIC; sum to 9 == Comprehensive
g byte basicSF = (sf1 + sf2 + sf3 + sf4 + sf5 + sf6 + sf7) == 7
g byte noSF = (sf1 + sf2 + sf3 + sf4 + sf5 + sf6 + sf7 + sf8 + sf9) == 0
*g byte threshSF = (sf1 + sf2 + sf3 + sf4 + sf5 + sf6 + sf7
g byte compSF = (sf1 + sf2 + sf3 + sf4 + sf5 + sf6 + sf7 + sf8 + sf9 ) == 9
la var basicSF "Facility meets basic requirements"
la var compSF "Facility meets comprehensive requirements"
la var noSF "Facility does not meet any basic requirements"

* Create EmONC categorical variable
g EmONC = 0
la var EmONC "International EmONC Status"
replace EmONC = 1 if basicSF == 1
replace EmONC = 2 if compSF == 1

* Create value labels and attach to variable
la de status 1 "Basic EmONC" 2 "Comprehensive EmONC" 0 "Non-EmONC"
la values EmONC status

* Verify that variable corresponds to pre-existing EmONC status
tab EmONC emoncsta, mi
tab EmONC emonc_st
* EmONC status verified

* Compare differences between intl EmONC status and Malawi definition
g byte tagged_bemonc = (emoncsta==1 & emonc_st!=1)

* Initialize function to calculate survey totals accounting for weighting
qui local required_file mwtotals
qui foreach x of local required_file { 
	 capture findfile `x'.do, path($pathdo)
		if _rc==601 {
			noi disp in red "Please verify `x'.do function has been included in the do files"
			* Create an exit conditions based on whether or not file is found.
			if _rc==601 exit = 1
		}
		else do "$pathdo/mwtotals.do"
		noi disp in yellow "Survey total function initialized"
	}
*end


****************
* Total Births * 
****************
/*  Module 4 contains information about total births by month 
	Decided on 12/17/2013 Call w/ Zoe that weights will be based on births
	Check proposed total birth variable by creating new one with row sums

	Total Deliveries: based on questions 14-18 for each month, need to aggregate
	across rows and columns to get total births per year
*/	
local month jan feb mar apr may jun jul aug sep oct nov dec
foreach x of local month {
	egen tmp`x' = rsum2(m4q14`x' m4q15`x' m4q16`x' m4q17`x' m4q18`x'), allmiss
	}
*end
egen Deliveries = rsum2(tmp*), allmiss 
la var Deliveries "Total annual deliveries at facility"
drop tmp*

* Replicate Table 3.8 using mwtotals function
* Create a district count for total deliveries that accounts for district weighting
* Total number of births in hopsitals plus sampled centres + extrapolated numbers
mwtotals Deliveries
table District_Assess, c(mean allDeliveries)

/*egen totDeliv = total(totDeliveries), by(District_Assess)
egen distDeliv = mean(totDeliveries) if hCentre==1, by(District_Assess)
g allDistDeliv = (distDeliv * nonsurveyeddeliveries) + totDeliv
replace allDistDeliv = round(allDistDeliv)
la var allDistDeliv "Number of births attended in facilities"
table District_Assess, c(mean allDistDeliv)*/

* Formula = (Total EmONC B Deliveries * (1 +weight)) + Total EmONC C Deliveries
/* NOTE: despite what spreadsheet claims, it appears emonc_st was used in calcuations
as the results are not reproducible using intl emonc status. Moreover, some variables used 
in the weighted scheme are decimals (1.3333 clinics?), not sure how these were derived. */
local emonc EmONC
egen foo = total(totDeliveries) if `emonc'==1, by(District_Assess)
egen totDelivB = mean(foo), by(District_Assess)
drop foo
egen foo = total(totDeliveries) if `emonc'==2, by(District_Assess)
egen totDelivEM =mean(foo), by(District_Assess)
drop foo
g foo = (totDelivB *(1+weight)) + totDelivEM
egen allDistDelivEM = mean(foo), by(District_Assess)
replace allDistDelivEM = round(allDistDelivEM)
la var allDistDelivEM "Number of births attended in EmONC facilities"
table District_Assess, c(mean allDistDelivEM)
table District_Assess hCentre `emonc', c(mean allDeliveries mean allDistDelivEM sum totDeliveries)


* Readiness of Facilities (Skip for now)*
**********************************
* Direct Obstetric complications *
**********************************

* Recreate table 3.10 (Met Needs for obstetric complications @ 15% rate)
* Double check direct complication total

/* NOTES: P. 36 of report provides indicator construction Met need EmONC Services
 ** direct_e = variable in SPSS data capturing met needs
	m4q24 -> Antepartum hemorrhage; 
	m4q25 -> Postpartum hemmorrhage; 
	m4q27 -> Prolonged/obstructed labor
	m4q29 -> Postpartum sepsis;
	m4q31 -> abortion complications
	m4q30 -> Severe pre-eclampsia/eclampsia
	m4q32 -> ectopic pregnancy
	m4q28 -> Ruptured uterus; 
However, after closer inspection the report actually uses all questions from m4q24-33
*/

/*local month jan feb mar apr may jun jul aug sep oct nov dec
foreach x of local month {
	g tmp`x' = (m4q24`x' + m4q25`x' + m4q27`x' + m4q29`x' ////
		+ m4q31`x' + m4q30`x' + m4q32`x' + m4q28`x')
	}
*end
egen directCompl = rsum2(tmp*), allmiss
drop tmp*
la var directCompl "Direct complications total"  */

local month jan feb mar apr may jun jul aug sep oct nov dec
foreach x of local month {
	egen tmp`x' = rsum2(m4q24`x' m4q25`x' m4q26`x' m4q27`x' m4q28`x' ////
		 m4q29`x'  m4q30`x' m4q31`x' m4q32`x'  m4q33`x'), allmiss
	}
*end
egen directCompl = rsum2(tmp*)
replace directCompl = 0 if directCompl==.
drop tmp*
la var directCompl "Direct complications total"

* Compare newly created variable versus one in dataset
table District_Assess hCentre emonc_st, c(sum directCompl)
table District_Assess two_grou emonc_st , c(sum direct_e)

* QC/QA check
g byte metNeedsQC = (directCompl == direct_e)
clist directCompl direct_e Name_facility_Assess if metNeedsQC ==0
* Non-matches are due to missingness of variables; we have set them to be 0;

*Run totals function to generate accurate totals for direct complications
* Cannot replicate figures in report!
mwtotals directCompl
mwtotals direct_e

*****************


**********************************
* Caesareans *
**********************************

/* NOTE: These only occured in hospitals so no weighting is necessary */
egen cesareanTot = rsum2(m4q18*), allmiss 
la var cesareanTot "Total annual cesareans"
assert cesarean == cesareanTot

table District_Assess, c(sum cesareanTot)
table District_Assess emonc_st  , c(sum cesareanTot)
table District_Assess EmONC  , c(sum cesareanTot)

***************************
* Deaths  - Direct & Indirect causes*
***************************
/* NOTE: Double check total_de variable for accounting*/
local month jan feb mar apr may jun jul aug sep oct nov dec
foreach x of local month {
	egen tmp`x' = rsum2(m4q39`x'  m4q40`x'  m4q41`x'  m4q42`x'  m4q43`x' ////
		 m4q44`x'  m4q45`x'  m4q46`x'  m4q47`x'  m4q48`x'), allmiss
	}
*end
egen directDeaths = rsum2(tmp*), allmiss
la var directDeaths "Total maternal deaths due to direct obstetric causes"
drop tmp*

*assert alldirec == directDeaths
mwtotals directDeaths
*br m4q39*  m4q40*  m4q41*  m4q42*  m4q43*  m4q44*  m4q45*  m4q46*  m4q47*  m4q48*
table District_Assess hCentre emonc_st, c(sum directDeaths)
table District_Assess hCentre EmONC, c(sum directDeaths)

* Indirect deaths due to Malaria, HIV, Anemia, Hep, other
local month jan feb mar apr may jun jul aug sep oct nov dec
foreach x of local month {
	egen tmp`x' = rsum2(m4q49`x'  m4q50`x'  m4q51`x'  m4q52`x'  m4q53`x'), allmiss
	}
*end
egen indirectDeaths = rsum2(tmp*), allmiss
la var indirectDeaths "Total maternal deaths due to indirect obstetric causes"
drop tmp*

/* NOTE: row sum calculation defaulted to missing if one obs had missing; Counts will differ.
br  m4q49*  m4q50*  m4q51*  m4q52*  m4q53*  v1563_a indirectDeaths Name_facility_Assess District_Assess if District_Assess=="Mangochi"
*/

* Run function and tabulate different EmONC tables
mwtotals indirectDeaths
table District_Assess hCentre emonc_st, c(sum indirectDeaths)
table District_Assess hCentre EmONC, c(sum indirectDeaths)

egen unspecDeaths = rsum2(m4q54*), allmiss
la var unspecDeaths "Total maternal deaths due to unknown/uspecified causes"

g allMatDeath = directDeaths + indirectDeaths + unspecDeaths
la var allMatDeath "All maternal deaths (direct + indirect + other)"

* Run function and tabulate different EmONC tables
mwtotals allMatDeath
table District_Assess hCentre emonc_st, c(sum allMatDeath)
table District_Assess hCentre EmONC, c(sum allMatDeath)

***************
* Stillbirths *
***************
/* NOTE: Using Module 4 questions 56, 57, 58, and 59 */
local month jan feb mar apr may jun jul aug sep oct nov dec
foreach x of local month {
	egen tmp`x' = rsum2(m4q56`x'  m4q57`x'  m4q58`x'  m4q59`x'), allmiss
	}
*end
egen stillBirths = rsum2(tmp*), allmiss
la var stillBirths "Total stillBirths"
drop tmp*

mwtotals stillBirths
table District_Assess hCentre emonc_st, c(sum stillBirths)
table District_Assess hCentre EmONC, c(sum stillBirths)

* Browse to see if aggregation issues exist in injested data
* br m4q56*  m4q57*  m4q58*  m4q59* sbtotal stillBirths District_Assess

**********************************
* Complication rates by district *
**********************************

/* NOTE: using module 4 question 25 - Direct Obstetric Complication Postpartum hemorrhage */
local month jan feb mar apr may jun jul aug sep oct nov dec
foreach x of local month {
	egen tmp`x' = rsum2(m4q25`x'), allmiss
	}
*end
egen postHemorrhage = rsum2(tmp*), allmiss
la var postHemorrhage "Total postpartum hemorrhage complications"
drop tmp*
mwtotals postHemorrhage


local month jan feb mar apr may jun jul aug sep oct nov dec
foreach x of local month {
	egen tmp`x' = rsum2(m4q30`x'), allmiss
	}
*end
egen preEclampsia = rsum2(tmp*), allmiss
la var preEclampsia "Total preEclampsia complications"
drop tmp*
mwtotals preEclampsia


*************************
* Midwifes per district *
*************************
mwtotals mod2q2g


*************************
* Availabilit of EmONC  *
*************************
* List EmONCs by District/Region
table District_Assess emonc_st
table District_Assess basicSF

* Calculate weighted EmONCS
egen totEm = total(basicSF), by(District_Assess)


/* NOTES: Births calculated in excel spreadsheet: Malawi_EmONC_Assessment

*Availability of services: Create a table of each facility, type, EmONC status, level */
decode mod1q3, gen(fac_type)
replace fac_type = proper(fac_type)
decode two_grou, gen(hosp_or_HC)
replace hosp_or_HC = proper(hosp_or_HC)


preserve
#delimit ;
keep fac_name hosp_or_HC sf* EmONC emoncsta emonc_st uniq_id 
	two_grou uniq_id_Assess Name_facility_Assess Facility_type_Assess 
	Ownership_Assess Region_name_HIS Region_code_SALB District_Assess 
	Long_final Lat_final Deliveries directCompl allMatDeath allstillBirths;
#delimit cr	
order uniq_id uniq_id_Assess fac_name Name_facility_Assess Facility_type_Assess ///
hosp_or_HC Ownership_Assess Region_name_HIS Region_code_SALB District_Assess 
order emonc_st emoncsta EmONC, last

* Write results to availability tab of spreadsheet
export excel using "$pathxls/Malawi_EmONC_Assessment_facilities", sheet("Availability") sheetmodify firstrow(variables)
restore
