"""Place names Plotly's choropleth `locationmode` can actually resolve.

px.choropleth silently drops any location it cannot match: pass it
"Google Ads" and "Facebook Ads" as country names and it returns a complete,
well-formed figure with a colour bar covering the real value range and not one
country shaded. `success: true`, a 12 KB HTML file, and a blank world map --
which is how a coverage sweep recorded a geo map of ad platforms as a PASS.

Nothing in the stack can be asked which locations matched: plotly.js resolves
them in the browser, and geopandas removed its bundled naturalearth dataset in
1.0. So the names live here. This is static ISO 3166-1 data, no network, no new
dependency -- the same approach `_US_STATES` in _adv_helpers.py already takes.

The list only ever drives a warning or a refusal to draw an empty map, so it is
deliberately generous: aliases plotly accepts ("USA", "Russia", "South Korea")
are included alongside the formal names, and anything this module does not
recognise for an unknown mode is reported as recognised rather than guessed at.
"""

from __future__ import annotations

import unicodedata

__all__ = ["ISO3_CODES", "US_STATE_CODES", "unrecognised_locations"]


def _fold(value: str) -> str:
    """Lowercase and strip accents, so Cote/Côte and Sao/São compare equal.

    The table is written unaccented; real data is not, and plotly accepts both.
    """
    decomposed = unicodedata.normalize("NFKD", str(value).strip().lower())
    return "".join(ch for ch in decomposed if not unicodedata.combining(ch))


# ISO 3166-1 alpha-3 -> English short name. Kept as one string so the table
# stays readable and diffable.
_ISO3166 = """
AFG Afghanistan|ALA Aland Islands|ALB Albania|DZA Algeria|ASM American Samoa
AND Andorra|AGO Angola|AIA Anguilla|ATA Antarctica|ATG Antigua and Barbuda
ARG Argentina|ARM Armenia|ABW Aruba|AUS Australia|AUT Austria|AZE Azerbaijan
BHS Bahamas|BHR Bahrain|BGD Bangladesh|BRB Barbados|BLR Belarus|BEL Belgium
BLZ Belize|BEN Benin|BMU Bermuda|BTN Bhutan|BOL Bolivia|BES Bonaire
BIH Bosnia and Herzegovina|BWA Botswana|BVT Bouvet Island|BRA Brazil
IOT British Indian Ocean Territory|BRN Brunei|BGR Bulgaria|BFA Burkina Faso
BDI Burundi|CPV Cabo Verde|KHM Cambodia|CMR Cameroon|CAN Canada
CYM Cayman Islands|CAF Central African Republic|TCD Chad|CHL Chile|CHN China
CXR Christmas Island|CCK Cocos Islands|COL Colombia|COM Comoros|COG Congo
COD Democratic Republic of the Congo|COK Cook Islands|CRI Costa Rica
CIV Cote d'Ivoire|HRV Croatia|CUB Cuba|CUW Curacao|CYP Cyprus|CZE Czechia
DNK Denmark|DJI Djibouti|DMA Dominica|DOM Dominican Republic|ECU Ecuador
EGY Egypt|SLV El Salvador|GNQ Equatorial Guinea|ERI Eritrea|EST Estonia
SWZ Eswatini|ETH Ethiopia|FLK Falkland Islands|FRO Faroe Islands|FJI Fiji
FIN Finland|FRA France|GUF French Guiana|PYF French Polynesia|GAB Gabon
GMB Gambia|GEO Georgia|DEU Germany|GHA Ghana|GIB Gibraltar|GRC Greece
GRL Greenland|GRD Grenada|GLP Guadeloupe|GUM Guam|GTM Guatemala|GGY Guernsey
GIN Guinea|GNB Guinea-Bissau|GUY Guyana|HTI Haiti|HND Honduras|HKG Hong Kong
HUN Hungary|ISL Iceland|IND India|IDN Indonesia|IRN Iran|IRQ Iraq|IRL Ireland
IMN Isle of Man|ISR Israel|ITA Italy|JAM Jamaica|JPN Japan|JEY Jersey
JOR Jordan|KAZ Kazakhstan|KEN Kenya|KIR Kiribati|PRK North Korea
KOR South Korea|KWT Kuwait|KGZ Kyrgyzstan|LAO Laos|LVA Latvia|LBN Lebanon
LSO Lesotho|LBR Liberia|LBY Libya|LIE Liechtenstein|LTU Lithuania
LUX Luxembourg|MAC Macao|MDG Madagascar|MWI Malawi|MYS Malaysia|MDV Maldives
MLI Mali|MLT Malta|MHL Marshall Islands|MTQ Martinique|MRT Mauritania
MUS Mauritius|MYT Mayotte|MEX Mexico|FSM Micronesia|MDA Moldova|MCO Monaco
MNG Mongolia|MNE Montenegro|MSR Montserrat|MAR Morocco|MOZ Mozambique
MMR Myanmar|NAM Namibia|NRU Nauru|NPL Nepal|NLD Netherlands|NCL New Caledonia
NZL New Zealand|NIC Nicaragua|NER Niger|NGA Nigeria|NIU Niue|NFK Norfolk Island
MKD North Macedonia|MNP Northern Mariana Islands|NOR Norway|OMN Oman
PAK Pakistan|PLW Palau|PSE Palestine|PAN Panama|PNG Papua New Guinea
PRY Paraguay|PER Peru|PHL Philippines|PCN Pitcairn|POL Poland|PRT Portugal
PRI Puerto Rico|QAT Qatar|REU Reunion|ROU Romania|RUS Russia|RWA Rwanda
BLM Saint Barthelemy|SHN Saint Helena|KNA Saint Kitts and Nevis
LCA Saint Lucia|MAF Saint Martin|SPM Saint Pierre and Miquelon
VCT Saint Vincent and the Grenadines|WSM Samoa|SMR San Marino
STP Sao Tome and Principe|SAU Saudi Arabia|SEN Senegal|SRB Serbia
SYC Seychelles|SLE Sierra Leone|SGP Singapore|SXM Sint Maarten|SVK Slovakia
SVN Slovenia|SLB Solomon Islands|SOM Somalia|ZAF South Africa
SGS South Georgia|SSD South Sudan|ESP Spain|LKA Sri Lanka|SDN Sudan
SUR Suriname|SJM Svalbard and Jan Mayen|SWE Sweden|CHE Switzerland|SYR Syria
TWN Taiwan|TJK Tajikistan|TZA Tanzania|THA Thailand|TLS Timor-Leste|TGO Togo
TKL Tokelau|TON Tonga|TTO Trinidad and Tobago|TUN Tunisia|TUR Turkey
TKM Turkmenistan|TCA Turks and Caicos Islands|TUV Tuvalu|UGA Uganda
UKR Ukraine|ARE United Arab Emirates|GBR United Kingdom|USA United States
URY Uruguay|UZB Uzbekistan|VUT Vanuatu|VAT Vatican City|VEN Venezuela
VNM Vietnam|VGB British Virgin Islands|VIR United States Virgin Islands
WLF Wallis and Futuna|ESH Western Sahara|YEM Yemen|ZMB Zambia|ZWE Zimbabwe
"""

# Names plotly's "country names" mode accepts that differ from the short name
# above, plus the spellings people actually type.
_ALIASES = """
USA|U.S.|U.S.A.|US|United States of America|America
UK|Great Britain|Britain|England|Scotland|Wales|Northern Ireland
Russian Federation|Korea, Republic of|Republic of Korea|Korea South
Korea, Democratic People's Republic of|Korea North|Czech Republic
Ivory Coast|Cote d Ivoire|Cabo Verde|Cape Verde|Burma|Swaziland|Macedonia
Republic of North Macedonia|Congo, Democratic Republic of the|DR Congo
Congo-Kinshasa|Congo-Brazzaville|Republic of the Congo|Holy See
Vatican|Timor Leste|East Timor|Laos People's Democratic Republic
Syrian Arab Republic|Viet Nam|Iran, Islamic Republic of|Bolivia
Plurinational State of Bolivia|Venezuela, Bolivarian Republic of
Tanzania, United Republic of|Moldova, Republic of|Brunei Darussalam
Micronesia, Federated States of|Palestine, State of|Taiwan, Province of China
Hong Kong SAR, China|Macao SAR, China|Netherlands (Kingdom of the)
Turkiye|The Gambia|The Bahamas|Trinidad & Tobago|Antigua & Barbuda
Bosnia & Herzegovina|St. Lucia|St. Kitts and Nevis|St. Vincent and the Grenadines
"""


def _build() -> tuple[frozenset[str], frozenset[str]]:
    codes: set[str] = set()
    names: set[str] = set()
    for entry in _ISO3166.replace("\n", "|").split("|"):
        entry = entry.strip()
        if not entry:
            continue
        code, _, name = entry.partition(" ")
        codes.add(code.upper())
        names.add(_fold(name))
    for alias in _ALIASES.replace("\n", "|").split("|"):
        alias = alias.strip()
        if alias:
            names.add(_fold(alias))
    return frozenset(codes), frozenset(names)


ISO3_CODES, _COUNTRY_NAMES = _build()

US_STATE_CODES = frozenset(
    """AL AK AZ AR CA CO CT DE FL GA HI ID IL IN IA KS KY LA ME MD MA MI MN MS
    MO MT NE NV NH NJ NM NY NC ND OH OK OR PA RI SC SD TN TX UT VT VA WA WV WI
    WY DC AS GU MP PR VI""".split()
)


def unrecognised_locations(values: list[str], mode: str) -> list[str]:
    """Values `mode` cannot place, so plotly would silently drop them.

    An unknown mode yields [] -- this module refuses to guess rather than
    reporting a valid map as broken.
    """
    normalised = (mode or "").strip().lower()
    if normalised in ("country names", "country_names"):
        return [v for v in values if _fold(v) not in _COUNTRY_NAMES]
    if normalised == "iso-3":
        return [v for v in values if str(v).strip().upper() not in ISO3_CODES]
    if normalised == "usa-states":
        return [v for v in values if str(v).strip().upper() not in US_STATE_CODES]
    return []
