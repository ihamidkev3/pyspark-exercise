# Brand settings
VALID_BRANDS = {'clp', 'okay', 'spar', 'dats', 'cogo'}

# Field matching patterns for data extraction
PATTERNS = {
    # Coordinate patterns
    'latitude': r'.*lat.*|.*latitude.*',
    'longitude': r'.*lon.*|.*lng.*|.*longitude.*',
    
    # Address patterns
    'postal': r'.*postal.*|.*zip.*|.*postcode.*',
    'house': r'.*house.*|.*number.*',
    'street': r'.*street.*|.*road.*|.*avenue.*|.*lane.*'
}