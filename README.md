# PySpark_Practice

# 🧪 PySpark Data Cleaning: Item_Fat_Content Standardization

This repository contains a PySpark notebook for cleaning and standardizing the `Item_Fat_Content` column in a retail dataset. The goal is to normalize variations in the fat content labels using regular expressions and sort the data accordingly.

---

## 📌 Features

- Load data using PySpark
- Clean inconsistent values in `Item_Fat_Content` using `regexp_replace`
- Replace:
  - `"Regular"` ➝ `"Reg"`
  - `"Low Fat"` ➝ `"Lf"`
- Sort data by the updated column
- Display cleaned output

---

## 🛠 Technologies Used

- Python 3.x
- Apache Spark (PySpark)
- Databricks (for development)
- GitHub (for version control)

---

## 🚀 How to Run

1. Clone the repository:
   ```bash
   git clone https://github.com/your-username/your-repo-name.git
