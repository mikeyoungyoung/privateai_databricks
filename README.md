# privateai_databricks
Examples of PII and PHI redaction in Databricks using the private-ai.com API

# Limitations and Backgound

PrivateAI PII/PHI NER models work best when entities have further context around then. For example `My SIN is 991 834 988` will return `My SIN is [SIN_1] `, but passing in just `991 834 988` may return `[PHONENUMBER_1]`. To use privateAI APIs on short form text it is better to pass in a column name and then the value like `SIN 901 934 092`.
</br>
</br>
However, there are obvisouly easier ways to handle columns dedicated to storing individual PII/PHI token and dropping or tokenizing the column may be the most appropriate
