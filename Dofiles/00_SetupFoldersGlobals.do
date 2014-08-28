/*-------------------------------------------------------------------------------
# Name:		00_SetupFolderGlobals
# Purpose:	Create series of folders For Malawi EmONC Assessment Analysis
# Author:	Tim Essam, Ph.D.
# Created:	08/19/2014
# Owner:	USAID GeoCenter | OakStream Systems, LLC
# License:	MIT License
# Ado(s):	see below
#-------------------------------------------------------------------------------
*/

/* Make directories for the study -- RUN/READ ME FIRST 
 1. Requires parent name for file folder structure
 2. Requires child names for sub-folders
 3. Sets global macros for the study; These are used through the do files.
*/

* Settings for all do files
set more off
set varabbrev off  // less confusing
set linesize 132   // use 7pt font for printing

	
* install the confirm directory ado if not already installed
/* Notes: Tried to be exhaustive here but may have missed a few add-ins. */
local required_ados confirmdir mvfiles fs spatgsa variog adolist labellist winsor2 polychoric estout scheme-burd ciplot
foreach x of local required_ados { 
	capture findfile `x'.ado
		if _rc==601 {
			cap ssc install `x'
		}
		else disp in yellow "`x' currently installed."
	}
*end

* Determine path for the study (needs to be modified)
global projectpath "U:\"
cd "$projectpath"

* Run a macro to set up study folder (needs to be modified)
* Name the file path below -- replace nigerlsms with your folder name
local pFolder MalawiEmONC
foreach dir in `pFolder' {
	confirmdir "`dir'"
	if `r(confirmdir)'==170 {
		mkdir "`dir'"
		display in yellow "Project directory named: `dir' created"
		}
	else disp as error "`dir' already exists, not created."
	cd "$projectpath\`dir'"
	}
* end

* Run initially to set up folder structure
* Choose your folders to set up as the local macro `folders'
local folders Rawdata Dofiles Datain Log Output Dataout Excel PDF Word Graph GIS Export R
foreach dir in `folders' {
	confirmdir "`dir'"
	if `r(confirmdir)'==170 {
			mkdir "`dir'"
			disp in yellow "`dir' successfully created."
		}
	else disp as error "`dir' already exists. Skipped to next folder."
}
*end

/*---------------------------------
# Set Globals based on path above #
-----------------------------------*/
global date $S_DATE
local dir `c(pwd)'
global path "`dir'"
global pathdo "`dir'\Dofiles"
global pathlog  "`dir'\Log"
global pathin "`dir'\Datain"
global pathout "`dir'\Dataout"
global pathgraph "`dir'\Graph"
global pathxls "`dir'\Excel"
global pathreg "`dir'\Output"
global pathgis "`dir'\GIS"
global pathraw "`dir'\Rawdata"
global pathexport "`dir'\Export"
global pathRin "`dir'\R"

* Project macros are defined as:
macro list 

/*------------------------------------------------------------
# Manually copy contents of LSMS folders into Datain Folder #
