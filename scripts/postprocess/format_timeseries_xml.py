"""
Creates env_timeseries.xml from user-specified input so that
the post-processing script matches your history output.
"""

import shutil

# CASETYPE = "coupPPE-hist"
# CASEPREFIX = "f.e22.FHIST_BGC.f19_f17_mg17"
# CASENUM = "000"
# CASE = f"{CASEPREFIX}.{CASETYPE}.{CASENUM}"

CASE = "f.e22.FHIST_BGC.f19_f17_mg17.coupPPE-hist.000"
POSTP_ROOT = f"/glade/derecho/scratch/bbuchovecky/postp/{CASE}/postprocess"

TIME_PERIOD = "years"
TIME_PERIOD_N = {
    "year_1": 100,
    "month_1": 100,
    "day_1": 10,
}

COMPONENTS = {
    "atm": "cam",
    "lnd": "clm2",
    "rof": "mosart",
    "glc": "cism",
    "ice": "cice",
}

HISTORY = {
    "atm": {
        "h0": ["month_1", "TRUE"],
        "h1": ["month_1", "FALSE"],
        "h2": ["day_1", "FALSE"],
        },
    "lnd": {
        "h0": ["month_1", "TRUE"],
        "h1": ["month_1", "FALSE"],
        "h2": ["day_1", "FALSE"],
        },
    "rof": {
        "h0": ["month_1", "FALSE"],
        },
    "glc": {
        "h": ["year_1", "FALSE"],
        },
    "ice": {
        "h": ["month_1", "FALSE"],
        },
}

XML_FORMAT = """
<!-- ===========================================================================  -->
<!-- env_timeseries.xml                                                           -->
<!--                                                                              -->
<!-- XML Element descriptions:                                                    -->
<!-- comp_archive_spec: name=[model component name included in history file]      -->
<!-- rootdir: component rootdir in $TIMESERIES_INPUT_ROOTDIR                      -->
<!-- multi_instance: NOT CURRENLTY IMPLEMENTED                                    -->
<!-- default_calendar: use this setting if time variable calendar attribute       -->
<!--                   is not defined                                             -->
<!-- files:                                                                       -->
<!--   file_extension: suffix regular expression for filename pattern match       -->
<!--   subdir: location of history files $TIMESERIES_INPUT_ROOTDIR/rootdir/subdir -->
<!--   tseries_create: flag to create history files for this suffix               -->
<!--         These settings are over-ridden using the env_postprocessing.xml      -->
<!--         setting TIMESERIES_GENERATE_ALL set to True or False.                -->
<!--   tseries_output_format: netcdf output format must be one of                 -->
<!--                          netcdf, netcdf4, netcdf4c, netcdfLarge              -->
<!--   tseries_tper: default time_period_freq if global attribute not set         -->
<!--   tseries_filecat_tper: what time period to include in a variable            -->
<!--                         timeseries chunk must be one of                      -->
<!--                         years, months, weeks, days, hours, mins              -->
<!--   tseries_filecat_n:    Number of concatenated tper chunks to include        -->
<!--                         in output variable timeseries file                   -->
<!-- tseries_time_variant_variables: list of time variant variables to be         -->
<!--                                 included in every output file                -->
<!--   variable: name of time variant variable                                    -->
<!--                                                                              -->
<!-- NOTE: all variable timeseries files are output to $TIMESERIES_OUTPUT_ROOTDIR -->
<!-- ===========================================================================  -->
"""

TIME_VARIANT_VARIABLES_ATM = """
    <tseries_time_variant_variables>
      <variable>ch4vmr</variable>
      <variable>co2vmr</variable>
      <variable>date</variable>
      <variable>date_written</variable>
      <variable>datesec</variable>
      <variable>f11vmr</variable>
      <variable>f12vmr</variable>
      <variable>n2ovmr</variable>
      <variable>ndcur</variable>
      <variable>nscur</variable>
      <variable>nsteph</variable>
      <variable>sol_tsi</variable>
      <variable>time</variable>
      <variable>time_bnds</variable>
      <variable>time_written</variable>
    </tseries_time_variant_variables>
"""

TIME_VARIANT_VARIABLES_LND_ROF = """
    <tseries_time_variant_variables>
      <variable>date_written</variable>
      <variable>mcdate</variable>
      <variable>mcsec</variable>
      <variable>mdcur</variable>
      <variable>mscur</variable>
      <variable>nstep</variable>
      <variable>time</variable>
      <variable>time_bounds</variable>
      <variable>time_written</variable>
    </tseries_time_variant_variables>
"""

TIME_VARIANT_VARIABLES_ICE = """
    <tseries_time_variant_variables>
      <variable>time</variable>
      <variable>time_bound</variable>
    </tseries_time_variant_variables>
"""

TIME_VARIANT_VARIABLES_GLC = """
    <tseries_time_variant_variables>
      <variable>time</variable>
    </tseries_time_variant_variables>
"""

TIME_VARIANT_VARIABLES = {
    "atm": TIME_VARIANT_VARIABLES_ATM,
    "lnd": TIME_VARIANT_VARIABLES_LND_ROF,
    "rof": TIME_VARIANT_VARIABLES_LND_ROF,
    "glc": TIME_VARIANT_VARIABLES_GLC,
    "ice": TIME_VARIANT_VARIABLES_ICE,
}


def create_hist_files(hist_n, to_create, tper, filecat_tper, filecat_n):
    """Create an xml decsriptor for a history output timeseries."""
    xml_block = []
    xml_block.append(f'      <file_extension suffix=".{hist_n}.[0-9]">')
    xml_block.append('        <subdir>hist</subdir>')
    xml_block.append(f'        <tseries_create>{to_create.upper()}</tseries_create>')
    xml_block.append('        <tseries_output_format>netcdf4c</tseries_output_format>')
    xml_block.append(f'        <tseries_tper>{tper}</tseries_tper>')
    xml_block.append(f'        <tseries_filecat_tper>{filecat_tper}</tseries_filecat_tper>')
    xml_block.append(f'        <tseries_filecat_n>{filecat_n}</tseries_filecat_n>')
    xml_block.append('      </file_extension>\n')
    xml_block = "\n".join(xml_block)
    return xml_block


print(f"{POSTP_ROOT}/env_timeseries.xml")
shutil.copy2(f"{POSTP_ROOT}/env_timeseries.xml", "env_timeseries_coupPPE.xml")

with open("env_timeseries_coupPPE.xml", "w", encoding="utf-8") as f:
    f.write('<?xml version="1.0"?>\n')
    f.write('<config_definition version="1.0">\n')
    f.write(XML_FORMAT)
    f.write('\n')
    f.write('<components>\n')

    for comp, stream in COMPONENTS.items():
        print(comp, stream)
        f.write(f'  <comp_archive_spec name="{stream}">\n')
        f.write(f'    <rootdir>{comp}</rootdir>\n')
        f.write('    <multi_instance>True</multi_instance>\n')
        f.write('    <default_calendar>noleap</default_calendar>\n')
        f.write('    <files>\n')

        for hist_num, tperiod in HISTORY[comp].items():
            print(" ", hist_num, tperiod[0], tperiod[1])
            f.write(create_hist_files(hist_num, tperiod[1], tperiod[0], TIME_PERIOD, TIME_PERIOD_N[tperiod[0]]))

        f.write('    </files>')
        f.write('    ' + TIME_VARIANT_VARIABLES[comp])
        f.write('  </comp_archive_spec>\n')
        f.write('\n\n')

    f.write('</components>\n')
    f.write('</config_definition>')

