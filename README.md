# Mineralogia

## Virtual environment

```bash
conda create -n mineralogia python=3.9.13
conda activate mineralogia
python -m pip install --upgrade pip setuptools wheel
python -m pip install -e ".[dev]"
pre-commit install
pre-commit autoupdate
```

## Directorio

```bash
src/
├── data.py       - utilidades de procesamiento de datos
├── evaluate.py   - componentes de evaluación
├── main.py       - operaciones de entrenamiento/optimización
├── predict.py    - utilidades de inferencia
├── train.py      - utilidades de entrenamiento
└── utils.py      - utilidades suplementarias
```

## Workflow

```bash
python src/main.py elt-data --file-name="20230201152433_diego.juanes@ypf.com_Nq.N.x-8.las"
python src/main.py optimize --args-fp="config/args.json" --study-name="optimization" --num-trials=10
python src/main.py train-model --args-fp="config/args.json" --experiment-name="baselines" --run-name="sgd"
python src/main.py predict "20230201152433_diego.juanes@ypf.com_Nq.N.x-8.las"
```

## API

```bash
uvicorn app.api:app --host 0.0.0.0 --port 8000 --reload --reload-dir src --reload-dir app  # dev
gunicorn -c app/gunicorn.py -k uvicorn.workers.UvicornWorker app.api:app  # prod
```
