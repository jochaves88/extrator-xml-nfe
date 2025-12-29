import os
import shutil
import zipfile
import glob
import pandas as pd
import xml.etree.ElementTree as ET
from fastapi import FastAPI, UploadFile, File, Form
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from sqlalchemy import create_engine, Column, String, Float, Integer, Date, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime

app = FastAPI()

# --- CONFIGURAÇÃO DO BANCO DE DADOS ---
# Pega a URL do docker-compose ou usa local para testes
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./test.db")
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# --- MODELO DA TABELA (A ESTRUTURA DOS DADOS) ---
class NFe(Base):
    __tablename__ = "notas_fiscais"
    
    # A Chave é a identidade única. Se repetir, atualizamos.
    chave = Column(String, primary_key=True, index=True)
    mes = Column(String)
    ano = Column(String)
    data_emissao = Column(Date)
    
    emitente_nome = Column(String)
    emitente_cnpj = Column(String)
    destinatario_nome = Column(String)
    
    numero_nf = Column(String)
    valor_total = Column(Float)
    
    # Dados do Produto (Resumido para o filtro, o detalhe JSON guarda o resto se precisar)
    produto_nome = Column(String)
    ncm = Column(String)
    cfop = Column(String)
    qtd = Column(Float)
    valor_unit = Column(Float)
    valor_total_item = Column(Float)
    
    # Impostos
    icms_total = Column(Float)
    ipi_total = Column(Float)
    # ... Podemos adicionar todas as 33 colunas aqui, mas vamos focar no essencial para o banco
    # Para o Excel final, vamos remontar tudo.
    
    # Guarda todos os dados brutos para gerar o excel completo depois
    dados_json = Column(String) 

# Cria a tabela no banco se não existir
Base.metadata.create_all(bind=engine)

# --- FUNÇÕES XML (MANTIDAS) ---
ns = {'nfe': 'http://www.portalfiscal.inf.br/nfe'}

def pegar_valor(no, caminho, tipo=str):
    if no is None: return tipo(0) if tipo in [float, int] else ""
    try:
        r = no.find(caminho, ns)
        if r is not None and r.text:
            return tipo(r.text.replace(',', '.'))
        return tipo(0) if tipo in [float, int] else ""
    except:
        return tipo(0) if tipo in [float, int] else ""

def extrair_dados_xml(arq):
    try:
        tree = ET.parse(arq)
        root = tree.getroot()
        if root.tag.endswith('nfeProc'):
            inf_nfe = root.find('nfe:NFe/nfe:infNFe', ns)
        else:
            inf_nfe = root.find('nfe:infNFe', ns)
        if inf_nfe is None: return []

        ide = inf_nfe.find('nfe:ide', ns)
        emit = inf_nfe.find('nfe:emit', ns)
        dest = inf_nfe.find('nfe:dest', ns)
        total = inf_nfe.find('nfe:total/nfe:ICMSTot', ns)
        
        chave = pegar_valor(root.find('nfe:protNFe/nfe:infProt', ns), 'nfe:chNFe')
        if not chave: chave = inf_nfe.attrib.get('Id', '')[3:]
        
        data_raw = pegar_valor(ide, 'nfe:dhEmi') or pegar_valor(ide, 'nfe:dEmi')
        data_nfe_dt = datetime.strptime(data_raw[:10], '%Y-%m-%d').date()
        
        # Loop itens
        itens_db = []
        dets = inf_nfe.findall('nfe:det', ns)
        
        # Colunas completas para o Excel
        bc_icms_tot = pegar_valor(total, 'nfe:vBC', float)
        icms_tot = pegar_valor(total, 'nfe:vICMS', float)
        bc_st_tot = pegar_valor(total, 'nfe:vBCST', float)
        icms_st_tot = pegar_valor(total, 'nfe:vST', float)
        desc_tot = pegar_valor(total, 'nfe:vDesc', float)
        ipi_tot = pegar_valor(total, 'nfe:vIPI', float)
        
        for i, det in enumerate(dets):
            prod = det.find('nfe:prod', ns)
            imposto = det.find('nfe:imposto', ns)
            
            # (Lógica simplificada de impostos item aqui para economizar espaço, 
            #  mas assume-se que você quer salvar TUDO no banco)
            
            # Vamos criar um dicionário COMPLETO para salvar no JSON e recuperar depois
            item_completo = {
                'Mês': str(data_nfe_dt.month).zfill(2),
                'Ano': str(data_nfe_dt.year),
                'Chave Acesso NFe': chave,
                'Inscrição Destinatário': pegar_valor(dest, 'nfe:IE'),
                'Inscrição Emitente': pegar_valor(emit, 'nfe:IE'),
                'Razão Social Emitente': pegar_valor(emit, 'nfe:xNome'),
                'Cnpj Emitente': pegar_valor(emit, 'nfe:CNPJ'),
                'UF Emitente': pegar_valor(emit, 'nfe:enderEmit/nfe:UF'),
                'Nr NFe': pegar_valor(ide, 'nfe:nNF'),
                'Série': pegar_valor(ide, 'nfe:serie'),
                'Data NFe': str(data_nfe_dt),
                'BC ICMS Total': bc_icms_tot,
                'ICMS Total': icms_tot,
                'BC ST Total': bc_st_tot,
                'ICMS ST Total': icms_st_tot,
                'Desc Total': desc_tot,
                'IPI Total': ipi_tot,
                'Total Produtos': pegar_valor(total, 'nfe:vProd', float),
                'Total NFe': pegar_valor(total, 'nfe:vNF', float),
                'Descrição Produto NFe': pegar_valor(prod, 'nfe:xProd'),
                'NCM na NFe': pegar_valor(prod, 'nfe:NCM'),
                'CFOP NFe': pegar_valor(prod, 'nfe:CFOP'),
                'Qtde': pegar_valor(prod, 'nfe:qCom', float),
                'Unid': pegar_valor(prod, 'nfe:uCom'),
                'Vr Unit': pegar_valor(prod, 'nfe:vUnCom', float),
                'Vr Total': pegar_valor(prod, 'nfe:vProd', float),
                'Desconto Item': pegar_valor(prod, 'nfe:vDesc', float),
                # ... Adicione os campos de imposto por item aqui se quiser
            }
            
            # Objeto para salvar no Banco (SQL)
            # Usamos chave + indice do item para criar chave unica do item
            nfe_db = NFe(
                chave=f"{chave}-{i+1}", # Chave composta para guardar cada item
                mes=str(data_nfe_dt.month).zfill(2),
                ano=str(data_nfe_dt.year),
                data_emissao=data_nfe_dt,
                emitente_nome=pegar_valor(emit, 'nfe:xNome'),
                emitente_cnpj=pegar_valor(emit, 'nfe:CNPJ'),
                numero_nf=pegar_valor(ide, 'nfe:nNF'),
                valor_total=pegar_valor(total, 'nfe:vNF', float),
                produto_nome=pegar_valor(prod, 'nfe:xProd'),
                dados_json=str(item_completo) # Salvamos o dicionario como texto para recuperar facil
            )
            itens_db.append(nfe_db)
            
        return itens_db
    except Exception as e:
        print(f"Erro XML {arq}: {e}")
        return []

# --- ROTAS ---

@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    temp_dir = "temp_files"
    if os.path.exists(temp_dir): shutil.rmtree(temp_dir)
    os.makedirs(temp_dir)
    
    zip_path = os.path.join(temp_dir, "arquivo.zip")
    with open(zip_path, "wb") as buffer: shutil.copyfileobj(file.file, buffer)
        
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref: zip_ref.extractall(temp_dir)
    except:
        return JSONResponse({"sucesso": False, "msg": "Erro no ZIP"}, 400)
    
    # Processar e Salvar no Banco
    arquivos = glob.glob(f"{temp_dir}/**/*.xml", recursive=True)
    arquivos += glob.glob(f"{temp_dir}/**/*.XML", recursive=True)
    
    session = SessionLocal()
    contador = 0
    try:
        for arq in arquivos:
            itens = extrair_dados_xml(arq)
            for item in itens:
                session.merge(item) # MERGE = Se existir atualiza, se não insere (Upsert)
                contador += 1
        session.commit()
    except Exception as e:
        session.rollback()
        return JSONResponse({"sucesso": False, "msg": f"Erro banco: {str(e)}"})
    finally:
        session.close()

    return JSONResponse({"sucesso": True, "msg": f"{contador} itens processados e salvos no banco!"})

@app.get("/dados-disponiveis")
async def get_dados():
    # Retorna quais anos e meses existem no banco para o filtro
    session = SessionLocal()
    try:
        # Busca anos distintos
        anos = session.query(NFe.ano).distinct().order_by(NFe.ano).all()
        lista_anos = [a[0] for a in anos]
        return {"anos": lista_anos}
    finally:
        session.close()

@app.post("/gerar-relatorio")
async def gerar_relatorio(anos: str = Form(...)):
    # Recebe "2024,2025" do form
    lista_anos_filtro = anos.split(',')
    
    session = SessionLocal()
    try:
        # Busca no banco filtrando pelos anos
        resultados = session.query(NFe).filter(NFe.ano.in_(lista_anos_filtro)).all()
        
        dados_excel = []
        for row in resultados:
            # Reconverte o texto JSON de volta para dicionario
            d = eval(row.dados_json)
            dados_excel.append(d)
            
        df = pd.DataFrame(dados_excel)
        
        # Ordenação de Colunas (Garante a sua ordem preferida)
        colunas_ordem = [
            'Mês', 'Ano', 'Chave Acesso NFe', 'Inscrição Destinatário', 'Inscrição Emitente', 
            'Razão Social Emitente', 'Cnpj Emitente', 'UF Emitente', 'Nr NFe', 'Série', 'Data NFe', 
            'BC ICMS Total', 'ICMS Total', 'BC ST Total', 'ICMS ST Total', 'Desc Total', 'IPI Total', 
            'Total Produtos', 'Total NFe', 'Descrição Produto NFe', 'NCM na NFe', 'CFOP NFe', 
            'Qtde', 'Unid', 'Vr Unit', 'Vr Total', 'Desconto Item'
        ]
        
        # Garante colunas
        if not df.empty:
            for col in colunas_ordem:
                if col not in df.columns: df[col] = ""
            df = df[colunas_ordem]
            
        output = "Relatorio_Final.xlsx"
        df.to_excel(output, index=False)
        return FileResponse(output, filename="Relatorio_Filtrado.xlsx")
        
    finally:
        session.close()

@app.get("/", response_class=HTMLResponse)
async def home():
    return """
    <!DOCTYPE html>
    <html lang="pt-br">
    <head>
        <meta charset="UTF-8">
        <title>Sistema Fiscal 4.0</title>
        <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap" rel="stylesheet">
        <style>
            body { font-family: 'Inter', sans-serif; background: #f1f5f9; padding: 20px; color: #334155; }
            .container { max-width: 900px; margin: 0 auto; display: grid; gap: 20px; }
            
            .card { background: white; padding: 30px; border-radius: 12px; box-shadow: 0 2px 4px rgba(0,0,0,0.05); }
            h2 { margin-top: 0; color: #0f172a; }
            
            /* Area Upload */
            .drop-zone { border: 2px dashed #cbd5e1; padding: 40px; text-align: center; border-radius: 8px; cursor: pointer; transition: 0.2s; }
            .drop-zone:hover { border-color: #3b82f6; background: #eff6ff; }
            
            /* Area Filtros */
            .filters { margin-top: 20px; padding: 20px; background: #f8fafc; border-radius: 8px; }
            .checkbox-group { display: flex; gap: 15px; flex-wrap: wrap; margin: 10px 0; }
            .checkbox-label { background: white; padding: 8px 16px; border: 1px solid #e2e8f0; border-radius: 20px; cursor: pointer; user-select: none; }
            .checkbox-label:has(input:checked) { background: #3b82f6; color: white; border-color: #3b82f6; }
            input[type="checkbox"] { display: none; }
            
            .btn { background: #3b82f6; color: white; border: none; padding: 12px 24px; border-radius: 6px; font-weight: bold; cursor: pointer; width: 100%; font-size: 16px; }
            .btn:disabled { background: #cbd5e1; }
            .btn-green { background: #10b981; }
            .btn-green:hover { background: #059669; }
            
            .status { margin-top: 10px; font-weight: 600; text-align: center; }
        </style>
    </head>
    <body>
        <div class="container">
            <div class="card">
                <h2>📂 1. Importar XMLs</h2>
                <p>Suba quantos arquivos ZIP quiser. O banco guarda tudo e remove duplicados.</p>
                <form id="uploadForm">
                    <div class="drop-zone" id="dropZone">
                        <span style="font-size: 30px">☁️</span><br>
                        Arraste ZIPs aqui
                        <input type="file" id="fileInput" accept=".zip" style="display:none">
                    </div>
                </form>
                <div id="uploadStatus" class="status"></div>
            </div>

            <div class="card">
                <h2>📊 2. Gerar Relatório</h2>
                <p>Selecione os anos que deseja incluir no Excel final:</p>
                
                <div id="loadingYears">Carregando dados do banco...</div>
                
                <form id="reportForm" action="/gerar-relatorio" method="post">
                    <div class="checkbox-group" id="yearsContainer">
                        </div>
                    <input type="hidden" name="anos" id="anosInput">
                    <button type="submit" class="btn btn-green" id="btnReport" disabled>Baixar Excel Consolidado</button>
                </form>
            </div>
        </div>

        <script>
            // --- LÓGICA DE UPLOAD ---
            const dropZone = document.getElementById('dropZone');
            const fileInput = document.getElementById('fileInput');
            
            dropZone.addEventListener('click', () => fileInput.click());
            dropZone.addEventListener('dragover', (e) => { e.preventDefault(); dropZone.style.borderColor = '#3b82f6'; });
            dropZone.addEventListener('dragleave', () => dropZone.style.borderColor = '#cbd5e1');
            dropZone.addEventListener('drop', (e) => {
                e.preventDefault();
                if(e.dataTransfer.files.length) handleUpload(e.dataTransfer.files[0]);
            });
            fileInput.addEventListener('change', () => { if(fileInput.files.length) handleUpload(fileInput.files[0]); });

            async function handleUpload(file) {
                const status = document.getElementById('uploadStatus');
                status.textContent = "Enviando e processando no Banco de Dados...";
                status.style.color = "blue";
                
                const formData = new FormData();
                formData.append("file", file);
                
                try {
                    const res = await fetch('/upload', { method: 'POST', body: formData });
                    const data = await res.json();
                    
                    if(data.sucesso) {
                        status.textContent = "✅ " + data.msg;
                        status.style.color = "green";
                        loadYears(); // Recarrega os filtros
                    } else {
                        status.textContent = "❌ " + data.msg;
                        status.style.color = "red";
                    }
                } catch(e) {
                    status.textContent = "Erro de conexão.";
                    status.style.color = "red";
                }
            }

            // --- LÓGICA DE FILTROS ---
            async function loadYears() {
                try {
                    const res = await fetch('/dados-disponiveis');
                    const data = await res.json();
                    
                    const container = document.getElementById('yearsContainer');
                    document.getElementById('loadingYears').style.display = 'none';
                    container.innerHTML = '';
                    
                    if(data.anos.length === 0) {
                        container.innerHTML = "Nenhum dado no banco ainda.";
                        return;
                    }

                    data.anos.forEach(ano => {
                        const label = document.createElement('label');
                        label.className = 'checkbox-label';
                        label.innerHTML = `<input type="checkbox" value="${ano}" onchange="checkBtn()"> ${ano}`;
                        container.appendChild(label);
                    });
                } catch(e) { console.log(e); }
            }

            // Ativa botão se tiver algo marcado
            window.checkBtn = function() {
                const checked = document.querySelectorAll('input[type="checkbox"]:checked');
                const btn = document.getElementById('btnReport');
                btn.disabled = checked.length === 0;
                
                // Prepara input hidden para envio
                const vals = Array.from(checked).map(c => c.value).join(',');
                document.getElementById('anosInput').value = vals;
            }

            // Carrega filtros ao abrir
            loadYears();
        </script>
    </body>
    </html>
    """
