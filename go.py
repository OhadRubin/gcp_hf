import os
TOKEN=...
USERNAME=...
os.system(f"GIT_LFS_SKIP_SMUDGE=1 git clone https://git:{TOKEN}@huggingface.co/datasets/bigcode/the-stack")
from datasets import load_dataset, disable_caching
from io_utils import write_to_file, deserialize, serialize
disable_caching()

LANG="python"
rep="https://huggingface.co/datasets/bigcode/the-stack/resolve/main/"

paths = [x.replace(f"/home/{USERNAME}/the-stack/",rep) for x in glob.glob(f"/home/{USERNAME}/the-stack/data/*/*")]
for path in paths:
    ds = load_dataset("parquet", data_files=paths[:1], split="train", num_proc=1, cache_dir="/tmp/",token=TOKEN)
    # ds = list(ds)
    break
    write_to_file(map(serialize, ds), output_path, batch_size=10)
