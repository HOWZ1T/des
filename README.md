# DES
This is a simple implementation of the Distinct Elements in Streams algorithm.

## USAGE:
```python
from des import DES

des = DES(threshold=100, seed=42)

# hamlet is an array of words from the text of Hamlet
#   definition of hamlet omitted for brevity
count = des.distinct(
    stream=hamlet,
    stream_size=len(hamlet)
)

print(f"the number of distinct words in Hamlet is {count}")
```

## REFERENCE PAPER:
- ["Distinct Elements in Streams: An Algorithm for the (Text) Book"](https://arxiv.org/pdf/2301.10191)