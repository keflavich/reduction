"""
Compare Fengwei's Sgr C catalog to ALMA-IMF
"""
from astropy.table import Table, vstack
import requests
import tarfile
import io
import numpy as np
import matplotlib.pyplot as plt
from glob import glob
import os
import re
import gzip
import time
from pathlib import Path
import itertools

# URLs for the data
xu_url = "https://zenodo.org/records/15068333/files/SgrB1off.dbcat_commonbeam_publish.txt?download=1"
xu_url2 = "https://zenodo.org/records/15068333/files/SgrC.dbcat_commonbeam_publish.txt?download=1"
xu_url3 = "https://zenodo.org/records/15068333/files/The20kmsCloud.dbcat_commonbeam_publish.txt?download=1"
almaimf_url = 'https://cdsarc.cds.unistra.fr/viz-bin/nph-Cat/tar.gz?J/A+A/690/A33'
almaimf_readme = 'https://cdsarc.cds.unistra.fr/ftp/J/A+A/690/A33/ReadMe'

SNR_THRESHOLD = 10  # Minimum signal-to-noise ratio for both 1mm and 3mm fluxes

distances = {'W43-MM1': 5.5,
             'W43-MM2': 5.5,
             'W43-MM3': 5.5,
             'W51-E': 5.4,
             'W51-IRS2': 5.4,
             'G333.60': 4.2,
             'G338.93': 3.9,
             'G010.62': 4.95,
             'G008.67': 3.4,
             'G012.80': 2.4,
             'G337.92': 2.7,
             'G327.29': 2.5,
             'G351.77': 2.0,
             'G353.41': 2.0,
             'G328.25': 2.5,
            }

# Cache settings
CACHE_DIR = Path('cache')
CACHE_DIR.mkdir(exist_ok=True)
XU_CACHE_FILE = CACHE_DIR / 'xu_data.txt'
XU_CACHE_FILE2 = CACHE_DIR / 'xu_data2.txt'
XU_CACHE_FILE3 = CACHE_DIR / 'xu_data3.txt'
ALMAIMF_CACHE_FILE = CACHE_DIR / 'almaimf_data.tar.gz'
CACHE_MAX_AGE = 24 * 60 * 60  # 24 hours in seconds

def download_if_needed(url, cache_file, max_age=CACHE_MAX_AGE):
    """Download data if cache doesn't exist or is too old."""
    if cache_file.exists():
        file_age = time.time() - cache_file.stat().st_mtime
        if file_age < max_age:
            print(f"Using cached data from {cache_file}")
            return cache_file.read_bytes()

    print(f"Downloading data from {url}")
    response = requests.get(url)
    response.raise_for_status()

    # Save to cache
    cache_file.write_bytes(response.content)
    return response.content

def parse_xu_data(content):
    """Parse Fengwei's data from LaTeX format."""
    text = content.decode('utf-8')

    # Extract table content between \startdata and \enddata
    start = text.find('\\startdata')
    end = text.find('\\enddata')
    if start == -1 or end == -1:
        raise ValueError('Could not find table data in Fengwei Xu file')
    table_data = text[start + len('\\startdata'):end].strip()

    # Get column names from \tablehead
    header_line = ''
    for line in text.splitlines():
        if '\\tablehead{' in line:
            header_line = line
            break

    # Extract column names and clean them
    colnames = re.findall(r'\\colhead\{([^}]*)\}', header_line)
    colnames = [name.strip() for name in colnames]

    # Parse the data lines
    data_rows = []
    for line in table_data.split('\n'):
        # Clean LaTeX artifacts
        line = line.replace('&', ' ').replace('\\\\', '').replace('$\\times$', 'x')
        # Split and clean values
        values = [v.strip() for v in line.split()]
        if len(values) == len(colnames):
            data_rows.append(values)

    # Create table
    table = Table(rows=data_rows, names=colnames)

    # Convert string columns to float where appropriate
    for col in table.colnames:
        try:
            table[col] = table[col].astype(float)
        except ValueError:
            continue

    # Extract fluxes and errors from the columns
    def extract_flux_and_error(value):
        if isinstance(value, str):
            # Extract number and error from format like "22.58(0.25)"
            match = re.match(r'([-+]?\d*\.?\d+)\((\d*\.?\d+)\)', value)
            if match:
                return float(match.group(1)), float(match.group(2))
            return float(value), 0.0
        return value, 0.0

    # Extract fluxes and errors
    f1mm, f1mm_err = zip(*[extract_flux_and_error(v) for v in table['FXPWE02']])
    f3mm, f3mm_err = zip(*[extract_flux_and_error(v) for v in table['FXPWE01']])

    # Convert to numpy arrays
    f1mm = np.array(f1mm)
    f3mm = np.array(f3mm)
    f1mm_err = np.array(f1mm_err)
    f3mm_err = np.array(f3mm_err)

    # Calculate SNR
    snr1mm = f1mm / f1mm_err
    snr3mm = f3mm / f3mm_err

    # Calculate alpha where both fluxes are positive and SNR > threshold
    mask = (f1mm > 0) & (f3mm > 0) & (snr1mm > SNR_THRESHOLD) & (snr3mm > SNR_THRESHOLD)
    alpha = np.full_like(f1mm, np.nan)
    alpha[mask] = np.log10(f1mm[mask]/f3mm[mask]) / -np.log10(226/93.6)

    # Add columns to table
    table['F1mm'] = f1mm * 1000  # Convert to mJy
    table['alpha_1mm_3mm'] = alpha
    table['SNR1mm'] = snr1mm
    table['SNR3mm'] = snr3mm

    return table

def parse_almaimf_data(content):
    """Parse ALMA-IMF data from fixed-width format."""
    content_io = io.BytesIO(content)
    with tarfile.open(fileobj=content_io, mode='r:') as tar:
        gs_members = [m for m in tar.getmembers() if m.name.endswith('gs.dat')]
        print(f"Found {len(gs_members)} gs.dat files")
        tables = []
        for member in gs_members:
            file_content = tar.extractfile(member).read().decode('utf-8')
            lines = [l for l in file_content.split('\n') if l.strip()]
            rows = []
            for line in lines:
                try:
                    # Extract all relevant columns based on fixed-width positions
                    cluster = line[0:8].strip()
                    seq = int(line[12:15].strip())
                    ra_deg = float(line[44:54].strip())
                    dec_deg = float(line[57:67].strip())

                    # 1.3mm data (01)
                    f1mm = float(line[122:131].strip()) * 1000  # Convert Jy/beam to mJy/beam
                    f1mm_err = float(line[134:142].strip()) * 1000
                    f1mm_tot = float(line[144:153].strip()) * 1000
                    f1mm_tot_err = float(line[156:164].strip()) * 1000
                    f1mm_alt = float(line[166:175].strip()) * 1000
                    f1mm_scale = float(line[178:186].strip())
                    f1mm_afwhm = float(line[189:197].strip())
                    f1mm_bfwhm = float(line[200:208].strip())
                    f1mm_asize = float(line[211:219].strip())
                    f1mm_bsize = float(line[222:230].strip())
                    f1mm_theta = float(line[233:241].strip())

                    # 3mm data (03)
                    f3mm = float(line[486:495].strip()) * 1000  # Convert Jy/beam to mJy/beam
                    f3mm_err = float(line[498:506].strip()) * 1000
                    f3mm_tot = float(line[508:517].strip()) * 1000
                    f3mm_tot_err = float(line[520:528].strip()) * 1000
                    f3mm_alt = float(line[530:539].strip()) * 1000
                    f3mm_scale = float(line[542:550].strip())
                    f3mm_afwhm = float(line[553:561].strip())
                    f3mm_bfwhm = float(line[564:572].strip())
                    f3mm_asize = float(line[575:583].strip())
                    f3mm_bsize = float(line[586:594].strip())
                    f3mm_theta = float(line[597:605].strip())

                    # Calculate SNR
                    snr1mm = f1mm / f1mm_err if f1mm_err > 0 else 0
                    snr3mm = f3mm / f3mm_err if f3mm_err > 0 else 0

                    # Calculate spectral index only if SNR > threshold for both bands
                    if f1mm > 0 and f3mm > 0 and snr1mm > SNR_THRESHOLD and snr3mm > SNR_THRESHOLD:
                        alpha = np.log10(f1mm/f3mm) / np.log10(226/95)
                    else:
                        alpha = np.nan

                    rows.append([
                        cluster, seq, ra_deg, dec_deg,
                        f1mm, f1mm_err, f1mm_tot, f1mm_tot_err, f1mm_alt,
                        f1mm_scale, f1mm_afwhm, f1mm_bfwhm, f1mm_asize, f1mm_bsize, f1mm_theta,
                        f3mm, f3mm_err, f3mm_tot, f3mm_tot_err, f3mm_alt,
                        f3mm_scale, f3mm_afwhm, f3mm_bfwhm, f3mm_asize, f3mm_bsize, f3mm_theta,
                        alpha, snr1mm, snr3mm
                    ])
                except Exception as e:
                    print(f"Error parsing line: {line}\nError: {e}")
                    continue

            if rows:
                table = Table(rows=rows, names=[
                    'Cluster', 'Seq', 'RA_deg', 'Dec_deg',
                    'F1mm', 'F1mm_err', 'F1mm_tot', 'F1mm_tot_err', 'F1mm_alt',
                    'F1mm_scale', 'F1mm_afwhm', 'F1mm_bfwhm', 'F1mm_asize', 'F1mm_bsize', 'F1mm_theta',
                    'F3mm', 'F3mm_err', 'F3mm_tot', 'F3mm_tot_err', 'F3mm_alt',
                    'F3mm_scale', 'F3mm_afwhm', 'F3mm_bfwhm', 'F3mm_asize', 'F3mm_bsize', 'F3mm_theta',
                    'alpha_1mm_3mm', 'SNR1mm', 'SNR3mm'
                ])
                tables.append(table)

        if tables:
            return vstack(tables)
        else:
            return Table(names=[
                'Cluster', 'Seq', 'RA_deg', 'Dec_deg',
                'F1mm', 'F1mm_err', 'F1mm_tot', 'F1mm_tot_err', 'F1mm_alt',
                'F1mm_scale', 'F1mm_afwhm', 'F1mm_bfwhm', 'F1mm_asize', 'F1mm_bsize', 'F1mm_theta',
                'F3mm', 'F3mm_err', 'F3mm_tot', 'F3mm_tot_err', 'F3mm_alt',
                'F3mm_scale', 'F3mm_afwhm', 'F3mm_bfwhm', 'F3mm_asize', 'F3mm_bsize', 'F3mm_theta',
                'alpha_1mm_3mm', 'SNR1mm', 'SNR3mm'
            ])

# Download and process Fengwei's data
print("Processing Fengwei's SgrB1off data...")
xu_content = download_if_needed(xu_url, XU_CACHE_FILE)
xu_table = parse_xu_data(xu_content)

print("Processing Fengwei's SgrC data...")
xu_content2 = download_if_needed(xu_url2, XU_CACHE_FILE2)
xu_table2 = parse_xu_data(xu_content2)

print("Processing Fengwei's 20kmsCloud data...")
xu_content3 = download_if_needed(xu_url3, XU_CACHE_FILE3)
xu_table3 = parse_xu_data(xu_content3)

# Download and process ALMA-IMF data
print('Processing ALMA-IMF data...')
almaimf_content = download_if_needed(almaimf_url, ALMAIMF_CACHE_FILE)
almaimf_table = parse_almaimf_data(almaimf_content)
assert len(almaimf_table) > 0

# Create the plot
plt.figure(figsize=(10, 6))

# Filter Xu's data for SNR > threshold
mask1 = (xu_table['SNR1mm'] > SNR_THRESHOLD) & (xu_table['SNR3mm'] > SNR_THRESHOLD)
mask2 = (xu_table2['SNR1mm'] > SNR_THRESHOLD) & (xu_table2['SNR3mm'] > SNR_THRESHOLD)
mask3 = (xu_table3['SNR1mm'] > SNR_THRESHOLD) & (xu_table3['SNR3mm'] > SNR_THRESHOLD)

plt.scatter(xu_table[mask1]['alpha_1mm_3mm'], xu_table[mask1]['F1mm'],
           label='Sgr B1off (Xu 2025)', alpha=0.6, color='blue')
plt.scatter(xu_table2[mask2]['alpha_1mm_3mm'], xu_table2[mask2]['F1mm'],
           label='Sgr C (Xu 2025)', alpha=0.6, color='blue')
plt.scatter(xu_table3[mask3]['alpha_1mm_3mm'], xu_table3[mask3]['F1mm'],
           label='20km/s Cloud (Xu 2025)', alpha=0.6, color='blue')

# Define markers for different distances
markers = ['o', 's', '^', 'D', 'v', '<', '>', 'p', '*', 'h', '8', 'P', 'X', 'd', '|']

# Plot ALMA-IMF data with different markers for each distance
for (cluster, distance), marker in zip(distances.items(), markers):
    mask = (almaimf_table['Cluster'] == cluster) & (almaimf_table['SNR1mm'] > SNR_THRESHOLD) & (almaimf_table['SNR3mm'] > SNR_THRESHOLD)
    plt.scatter(almaimf_table[mask]['alpha_1mm_3mm'],
               almaimf_table[mask]['F1mm'] * 8**2 / distance**2,
               marker=marker,
               label=f'{cluster} ({distance}kpc)',
               alpha=0.6,
               color='orange')

#plt.scatter(almaimf_table['alpha_1mm_3mm'], almaimf_table['F1mm'],
#           label='ALMA-IMF (Louvet 2024)', alpha=0.6, color='orange')

plt.yscale('log')
plt.xlabel('Spectral Index (1mm-3mm)')
plt.ylabel('1mm Flux (mJy)')
plt.title(f'Spectral Index vs 1mm Flux Comparison (SNR > {SNR_THRESHOLD})')
plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
plt.xlim(-2, 5)
#plt.grid(True, alpha=0.3)

# Save the plot
plt.savefig('spectral_index_comparison.png', dpi=300, bbox_inches='tight')
#plt.close()

print("Plot saved as 'spectral_index_comparison.png'")