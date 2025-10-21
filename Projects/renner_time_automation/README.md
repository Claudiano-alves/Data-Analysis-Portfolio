# 🇧🇷 Automação de Extração e Armazenamento de Dados

## Introdução

Este projeto de automação foi desenvolvido para executar uma rotina diária de extração de dados corporativos. O sistema realiza consultas automatizadas ao banco de dados, processa as informações obtidas e armazena os resultados em arquivos CSV, salvando-os em uma pasta compartilhada na rede da empresa. O objetivo é garantir a disponibilidade consistente dos dados, eliminando a necessidade de execução manual diária.

## 🔧 Funcionalidades

### 1. Identificação e Configuração de Consultas
O script identifica automaticamente a consulta SQL armazenada na rede da empresa, garantindo que sempre utilize a versão mais atualizada das queries configuradas.

### 2. Manipulação Inteligente de Datas
O sistema manipula automaticamente os parâmetros de data necessários para a consulta, ajustando-os conforme o dia da execução. Isso garante que os dados extraídos correspondam ao período correto sem necessidade de intervenção manual.

### 3. Execução de Consultas
Realiza a conexão com o banco de dados e executa as consultas configuradas, garantindo a extração completa e precisa dos dados necessários.

### 4. Armazenamento em CSV
Os dados provenientes do banco de dados são processados e salvos em formato CSV.

### 5. Salvamento em Rede
Os arquivos gerados são automaticamente salvos na pasta compartilhada na rede da empresa.

## Benefícios

- **Automação completa**: Elimina processos manuais repetitivos
- **Consistência**: Garante que os dados sejam extraídos diariamente no mesmo padrão
- **Disponibilidade**: Mantém os dados sempre atualizados e acessíveis na rede
- **Redução de erros**: Minimiza erros humanos no processo de extração
- **Economia de tempo**: Libera a equipe para atividades mais estratégicas

## Agendamento

A automação está configurada para executar diariamente em horário pré-definido, garantindo que os dados estejam disponíveis no início do expediente.

---

**Desenvolvido para otimizar processos e garantir a disponibilidade de dados corporativos**

---
---

# 🇺🇸 Data Extraction and Storage Automation

## Introduction

This automation project was developed to execute a daily routine for extracting corporate data. The system performs automated database queries, processes the obtained information, and stores the results in CSV files, saving them to a shared folder on the company network. The objective is to ensure consistent data availability, eliminating the need for daily manual execution.

## 🔧 Features

### 1. Query Identification and Configuration
The script automatically identifies the SQL query stored on the company network, ensuring it always uses the most up-to-date version of the configured queries.

### 2. Intelligent Date Manipulation
The system automatically manipulates the date parameters required for the query, adjusting them according to the execution day. This ensures that the extracted data corresponds to the correct period without manual intervention.

### 3. Query Execution
Connects to the database and executes the configured queries, ensuring complete and accurate extraction of the required data.

### 4. CSV Storage
Data from the database is processed and saved in CSV format.

### 5. Network Storage
Generated files are automatically saved to the shared folder on the company network.

## Benefits

- **Complete automation**: Eliminates repetitive manual processes
- **Consistency**: Ensures data is extracted daily in the same standard
- **Availability**: Keeps data always updated and accessible on the network
- **Error reduction**: Minimizes human errors in the extraction process
- **Time savings**: Frees the team for more strategic activities

## Scheduling

The automation is configured to run daily at a predefined time, ensuring data is available at the beginning of the workday.

---

**Developed to optimize processes and ensure corporate data availability**