# 项目搭建

## 1. 创建项目

```shell
conda create -n myproject python=3.9
```

## 2. 激活项目

```shell
conda activate myproject
```

## 3. 安装依赖

```shell
conda install numpy pandas

conda list --export > requirements.txt  # 导出conda环境的所有包

# 或

pip freeze > requirements.txt           # 导出pip安装的包

pip install -r requirements.txt
```

## 4. 安装项目

```shell
pip install -e .
```

## 5. 运行项目

```shell
