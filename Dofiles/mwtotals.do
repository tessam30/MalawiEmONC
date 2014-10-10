* Create a function to calculate weighted variables. This program is called numerous times in the 01_DataProcessing do file. It is based on guidance in the 2010 Final Report (http://www.mamaye.org/sites/default/files/evidence/Em%20OC%20Report%20Malawi%202010%20CM.docx).


capture program drop mwtotals
program define mwtotals
set more off
	foreach var of local 0 {
		* Totals in all facilities (hosp & HCs)
		egen tot`var' = total(`var'), by(District_Assess)
		
		* Average in health clinics 
		egen dist`var' = mean(`var') if hCentre == 1, by(District_Assess)
		
		* Extrapolated totals to non-surveyed facilities + totals 
		g all`var' = ((dist`var' * nonsurveyeddeliveriesD))+ tot`var'
		
		* Round figures up to the nearest integer value
		replace all`var' = round(all`var', 1)
		
		* label newly created variable and create summary table
		la var all`var' "Survey adjusted total"
		table District_Assess, c(mean all`var')
	}
end

