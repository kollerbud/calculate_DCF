import sys
from pathlib import Path
# ../ROOT_DIR/src/finace_section
# ../ROOT_DIR/src/news_section
sys.path += [str(Path(__file__).parent/'finance_section'),
             str(Path(__file__).parent/'news_section'),
             ]