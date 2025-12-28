const axios = require('axios');
const XLSX = require('xlsx');
const fs = require('fs');
const path = require('path');
require('dotenv').config();

const KEY_BEARER = process.env.KEY_BEARER;
const BASE_URL = process.env.BASE_URL;

// Nome do arquivo Excel na raiz do projeto
const EXCEL_FILE_NAME = 'pessoas.xlsx';

// Função para ler o arquivo Excel
function lerArquivoExcel() {
    try {
        console.log(`📊 Lendo arquivo: ${EXCEL_FILE_NAME}`);
        
        // Verificar se arquivo existe
        if (!fs.existsSync(EXCEL_FILE_NAME)) {
            console.error(`❌ Arquivo ${EXCEL_FILE_NAME} não encontrado na raiz do projeto.`);
            console.log('📝 Crie um arquivo Excel com nome "pessoas.xlsx" contendo os dados.');
            console.log('📋 Estrutura esperada:');
            console.log('   - Colunas: nome, sobrenome, cpf, telefone, data_nascimento, genero, email, endereco, bairro, numero, cep, cidade, estado, estado_civil, escolaridade, employments, groups, status_batismo, data_batismo, observacoes, nome_pai, nome_mae, cidade_natal, profissao, trabalha, ministerio_0, ministerio_1, tipo_membro_0, tipo_membro_1, tipo_membro_2, tipo_membro_3, escala_0, escala_1, escala_2, escala_3, escala_4, escala_5, escala_6');
            process.exit(1);
        }
        
        // Ler o arquivo Excel
        const workbook = XLSX.readFile(EXCEL_FILE_NAME);
        const primeiraPlanilha = workbook.SheetNames[0];
        const worksheet = workbook.Sheets[primeiraPlanilha];
        
        // Converter para JSON
        const dados = XLSX.utils.sheet_to_json(worksheet);
        
        if (dados.length === 0) {
            console.error('❌ Nenhum dado encontrado no arquivo Excel.');
            process.exit(1);
        }
        
        console.log(`✅ Encontradas ${dados.length} pessoas no arquivo`);
        return dados;
        
    } catch (error) {
        console.error('❌ Erro ao ler arquivo Excel:', error.message);
        throw error;
    }
}

// Converter dados do Excel para formato da API
function converterParaFormatoAPI(dadosExcel) {
    return {
        avatar_file: "",
        first_name: dadosExcel.nome || "",
        last_name: dadosExcel.sobrenome || "",
        password: "",
        employments: dadosExcel.employments ? dadosExcel.employments.toString().split(',').map(Number).filter(n => !isNaN(n)) : [257290],
        groups: dadosExcel.groups ? dadosExcel.groups.toString().split(',').map(Number).filter(n => !isNaN(n)) : [45962],
        categories: [],
        gender: dadosExcel.genero || "F",
        phone_1: dadosExcel.telefone || "",
        phone_2: "",
        email: dadosExcel.email || "",
        address_1: dadosExcel.endereco || "",
        address_2: dadosExcel.bairro || "",
        address_number: dadosExcel.numero || "",
        postal_code: dadosExcel.cep || "",
        id_city: dadosExcel.cidade ? parseInt(dadosExcel.cidade) : 71917,
        id_state: dadosExcel.estado || "Goias",
        id_country: "BR",
        birthydate: dadosExcel.data_nascimento || "",
        doc_1: dadosExcel.cpf || "",
        doc_2: "",
        marital_status: dadosExcel.estado_civil || "single",
        scholarity: dadosExcel.escolaridade || "",
        spouse_name: "",
        spouse_christian: "",
        conversion_date: "",
        baptism_status: dadosExcel.status_batismo ? parseInt(dadosExcel.status_batismo) : 0,
        baptism_date: dadosExcel.data_batismo || "",
        notes: dadosExcel.observacoes || "",
        extrafields: {
            "15819": dadosExcel.nome_pai || "",
            "15820": dadosExcel.nome_mae || "",
            "15823": dadosExcel.cidade_natal || "",
            "15825": dadosExcel.profissao || "",
            "15826": dadosExcel.trabalha === "true" || dadosExcel.trabalha === true || dadosExcel.trabalha === 1,
            "15821_0": dadosExcel.ministerio_0 === "true" || dadosExcel.ministerio_0 === true || dadosExcel.ministerio_0 === 1,
            "15821_1": dadosExcel.ministerio_1 === "true" || dadosExcel.ministerio_1 === true || dadosExcel.ministerio_1 === 1,
            "17295_0": dadosExcel.tipo_membro_0 === "true" || dadosExcel.tipo_membro_0 === true || dadosExcel.tipo_membro_0 === 1,
            "17295_1": dadosExcel.tipo_membro_1 === "true" || dadosExcel.tipo_membro_1 === true || dadosExcel.tipo_membro_1 === 1,
            "17295_2": dadosExcel.tipo_membro_2 === "true" || dadosExcel.tipo_membro_2 === true || dadosExcel.tipo_membro_2 === 1,
            "17295_3": dadosExcel.tipo_membro_3 === "true" || dadosExcel.tipo_membro_3 === true || dadosExcel.tipo_membro_3 === 1,
            "15822_0": dadosExcel.escala_0 === "true" || dadosExcel.escala_0 === true || dadosExcel.escala_0 === 1,
            "15822_1": dadosExcel.escala_1 === "true" || dadosExcel.escala_1 === true || dadosExcel.escala_1 === 1,
            "15822_2": dadosExcel.escala_2 === "true" || dadosExcel.escala_2 === true || dadosExcel.escala_2 === 1,
            "15822_3": dadosExcel.escala_3 === "true" || dadosExcel.escala_3 === true || dadosExcel.escala_3 === 1,
            "15822_4": dadosExcel.escala_4 === "true" || dadosExcel.escala_4 === true || dadosExcel.escala_4 === 1,
            "15822_5": dadosExcel.escala_5 === "true" || dadosExcel.escala_5 === true || dadosExcel.escala_5 === 1,
            "15822_6": dadosExcel.escala_6 === "true" || dadosExcel.escala_6 === true || dadosExcel.escala_6 === 1
        }
    };
}

// Função para enviar pessoa para API
async function enviarPessoaParaAPI(pessoa, indice, total) {
    console.log(`\n📤 [${indice + 1}/${total}] Enviando: ${pessoa.first_name} ${pessoa.last_name}`);
    
    const authHeader = KEY_BEARER.startsWith('Bearer ') 
        ? KEY_BEARER 
        : `Bearer ${KEY_BEARER}`;
    
    const headers = {
        Authorization: authHeader,
        Accept: 'application/json, text/plain, */*',
        'Content-Type': 'application/json',
        Referer: 'https://app.enuves.com/',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    };

    try {
        const response = await axios({
            method: 'post',
            url: `${BASE_URL}/people`,
            headers: headers,
            data: pessoa,
            timeout: 30000
        });

        console.log(`✅ [${indice + 1}/${total}] Criada com sucesso! ID: ${response.data.id || 'N/A'}`);
        return { success: true, data: response.data };
        
    } catch (error) {
        console.error(`❌ [${indice + 1}/${total}] Erro: ${error.response?.data?.message || error.message}`);
        
        if (error.response) {
            console.log(`   Status: ${error.response.status}`);
            console.log(`   Detalhes:`, JSON.stringify(error.response.data, null, 2));
        }
        
        return { 
            success: false, 
            error: error.response?.data || error.message,
            pessoa: `${pessoa.first_name} ${pessoa.last_name}`
        };
    }
}

// Função principal
async function processarPessoas() {
    try {
        console.log('🚀 INICIANDO PROCESSAMENTO DE PESSOAS');
        console.log('====================================');
        
        // Verificar configurações
        if (!KEY_BEARER) {
            throw new Error('KEY_BEARER não configurado no .env');
        }
        if (!BASE_URL) {
            throw new Error('BASE_URL não configurado no .env');
        }
        
        console.log(`🔑 Token: ${KEY_BEARER.substring(0, 20)}...`);
        console.log(`🌐 URL Base: ${BASE_URL}`);
        
        // 1. Ler dados do Excel
        const dadosExcel = lerArquivoExcel();
        
        // 2. Converter para formato da API
        const pessoas = dadosExcel.map(dados => converterParaFormatoAPI(dados));
        
        console.log(`\n🔄 Convertidas ${pessoas.length} pessoas para formato da API`);
        
        // 3. Enviar cada pessoa para API
        const resultados = [];
        
        for (let i = 0; i < pessoas.length; i++) {
            const pessoa = pessoas[i];
            
            // Validar dados mínimos
            if (!pessoa.first_name || !pessoa.last_name || !pessoa.doc_1) {
                console.warn(`⚠️  [${i + 1}/${pessoas.length}] Dados incompletos, pulando...`);
                resultados.push({ 
                    success: false, 
                    pessoa: `${pessoa.first_name} ${pessoa.last_name}`,
                    error: 'Dados incompletos (nome, sobrenome ou CPF faltando)' 
                });
                continue;
            }
            
            console.log(`\n📝 [${i + 1}/${pessoas.length}] Processando: ${pessoa.first_name} ${pessoa.last_name}`);
            console.log(`   CPF: ${pessoa.doc_1}`);
            console.log(`   Telefone: ${pessoa.phone_1}`);
            console.log(`   E-mail: ${pessoa.email || '(não informado)'}`);
            
            // Enviar para API
            const resultado = await enviarPessoaParaAPI(pessoa, i, pessoas.length);
            resultados.push(resultado);
            
            // Aguardar 1 segundo entre requisições para evitar rate limiting
            if (i < pessoas.length - 1) {
                console.log(`⏳ Aguardando 1 segundo antes do próximo envio...`);
                await new Promise(resolve => setTimeout(resolve, 1000));
            }
        }
        
        // 4. Exibir resumo
        console.log('\n📊 RESUMO DO PROCESSAMENTO');
        console.log('========================');
        
        const sucessos = resultados.filter(r => r.success).length;
        const falhas = resultados.filter(r => !r.success).length;
        
        console.log(`✅ Sucessos: ${sucessos}`);
        console.log(`❌ Falhas: ${falhas}`);
        console.log(`📝 Total processado: ${resultados.length}`);
        
        if (falhas > 0) {
            console.log('\n🔍 Detalhes das falhas:');
            resultados.filter(r => !r.success).forEach((r, index) => {
                console.log(`${index + 1}. ${r.pessoa || 'Sem nome'}: ${r.error?.message || r.error}`);
            });
        }
        
        // Salvar log em arquivo
        salvarLog(resultados);
        
        return resultados;
        
    } catch (error) {
        console.error('❌ ERRO NO PROCESSAMENTO:', error.message);
        throw error;
    }
}

// Salvar log em arquivo
function salvarLog(resultados) {
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
    const logFileName = `log_processamento_${timestamp}.json`;
    
    const logData = {
        data: new Date().toISOString(),
        total: resultados.length,
        sucessos: resultados.filter(r => r.success).length,
        falhas: resultados.filter(r => !r.success).length,
        detalhes: resultados.map(r => ({
            pessoa: r.pessoa || r.data?.first_name + ' ' + r.data?.last_name,
            sucesso: r.success,
            id: r.data?.id,
            erro: r.error?.message || r.error
        }))
    };
    
    fs.writeFileSync(logFileName, JSON.stringify(logData, null, 2));
    console.log(`📝 Log salvo em: ${logFileName}`);
}


// Executar
setTimeout(() => {
  (async () => {
    try {
        await processarPessoas();
        console.log('\n🎉 Processamento concluído!');
    } catch (error) {
        console.error('\n💥 Processamento interrompido com erro:', error.message);
        process.exit(1);
    }
  })();
}, 10000);