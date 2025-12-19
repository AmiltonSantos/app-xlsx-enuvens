const axios = require('axios');
const xlsx = require('xlsx');
require('dotenv').config();

const KEY_BEARER = process.env.KEY_BEARER;
const BASE_URL = process.env.BASE_URL;
const BASE_URL_GROUPS = process.env.BASE_URL_GROUPS;

// Cache global
const cache = {
    people: new Map(),
    groups: new Map()
};

// Configurações otimizadas
const CONFIG = {
    MAX_CONCURRENT_REQUESTS: 100,  // Aumentado para máximo paralelismo
    REQUEST_TIMEOUT: 45000,
    RETRY_ATTEMPTS: 2,
    DELAY_BETWEEN_BATCHES: 100
};

const delay = ms => new Promise(resolve => setTimeout(resolve, ms));

// Função de retry com backoff
async function fetchWithRetry(url, config, retries = CONFIG.RETRY_ATTEMPTS) {
    for (let attempt = 1; attempt <= retries; attempt++) {
        try {
            return await axios.get(url, config);
        } catch (error) {
            if (attempt === retries) throw error;
            await delay(Math.pow(2, attempt) * 1000);
        }
    }
}

async function fetchData() {
    console.time('🚀 Tempo total de execução');
    console.log('🎯 Iniciando processamento ULTRA RÁPIDO...');
    
    try {
        if (!BASE_URL_GROUPS || !KEY_BEARER) {
            throw new Error('Variáveis de ambiente não configuradas');
        }

        const axiosConfig = {
            headers: {
                'Authorization': KEY_BEARER,
                'Accept': 'application/json',
                'Content-Type': 'application/json'
            },
            timeout: CONFIG.REQUEST_TIMEOUT
        };

        // 1. Buscar TODOS os grupos de uma vez
        console.log('🔍 Buscando todos os grupos...');
        const allGroups = await fetchAllGroups(axiosConfig);
        console.log(`✅ ${allGroups.length} grupos encontrados`);

        // 2. Coletar TODOS os IDs de pessoas únicas de TODOS os grupos
        console.log('📋 Coletando TODOS os IDs de pessoas...');
        const allPeopleIds = await collectAllPeopleIds(allGroups, axiosConfig);
        console.log(`👥 ${allPeopleIds.length} IDs únicos de pessoas coletados`);

        // 3. Buscar TODAS as pessoas de UMA VEZ em paralelo máximo
        console.log('⚡ Buscando TODAS as pessoas em paralelo máximo...');
        const allPeopleData = await fetchAllPeopleInParallel(allPeopleIds, axiosConfig);
        console.log(`📊 ${allPeopleData.length} pessoas processadas`);

        // 4. Criar mapa rápido de acesso por ID
        const peopleMap = new Map();
        allPeopleData.forEach(person => {
            if (person && person.id) {
                peopleMap.set(person.id, person);
            }
        });

        // 5. Gerar dados do Excel processando grupos
        console.log('📝 Gerando dados para Excel...');
        const rows = await generateExcelData(allGroups, peopleMap, axiosConfig);

        // 6. Criar arquivo Excel
        console.log('💾 Criando arquivo Excel...');
        await generateExcelFile(rows);

        console.timeEnd('🚀 Tempo total de execução');
        console.log(`✨ Processamento concluído! ${rows.length - 1} linhas geradas.`);

    } catch (error) {
        console.error('❌ Erro:', error.message);
        return null;
    }
}

// 1. Buscar todos os grupos
async function fetchAllGroups(config) {
    try {
        const response = await fetchWithRetry(BASE_URL_GROUPS, config);
        return response.data.results || [];
    } catch (error) {
        throw new Error(`Falha ao buscar grupos: ${error.message}`);
    }
}

// 2. Coletar todos os IDs de pessoas únicas
async function collectAllPeopleIds(groups, config) {
    const allPeopleIds = new Set();
    
    // Buscar membros de todos os grupos EM PARALELO
    const groupPromises = groups.map(async (group) => {
        try {
            const response = await axios.get(
                `${BASE_URL}/groups/${group.id}`,
                config
            );

            const membrosData = response.data.results;
            if (membrosData && membrosData.peoples) {
                const peoples = JSON.parse(membrosData.peoples || '[]');
                peoples.forEach(id => allPeopleIds.add(id));
            }
        } catch (error) {
            console.warn(`   ⚠️  Erro no grupo ${group.name}: ${error.message}`);
        }
    });

    await Promise.allSettled(groupPromises);
    return Array.from(allPeopleIds);
}

// 3. Buscar TODAS as pessoas em paralelo máximo - CORAÇÃO DO SISTEMA
async function fetchAllPeopleInParallel(personIds, config) {
    if (personIds.length === 0) return [];
    
    const total = personIds.length;
    console.log(`   🚀 Iniciando ${total} requisições paralelas...`);
    
    // Divide em chunks gerenciáveis
    const chunkSize = CONFIG.MAX_CONCURRENT_REQUESTS;
    const allResults = [];
    
    for (let i = 0; i < personIds.length; i += chunkSize) {
        const chunkIds = personIds.slice(i, i + chunkSize);
        const chunkNumber = Math.floor(i / chunkSize) + 1;
        const totalChunks = Math.ceil(personIds.length / chunkSize);
        
        console.log(`   📦 Processando chunk ${chunkNumber}/${totalChunks} (${chunkIds.length} pessoas)`);
        
        // Cria TODAS as promises deste chunk
        const chunkPromises = chunkIds.map((personId, index) => 
            fetchSinglePersonWithDelay(personId, config, index * 10)
        );
        
        // Executa TODAS as promises em paralelo
        const chunkResults = await Promise.allSettled(chunkPromises);
        
        // Processa resultados
        const successfulResults = chunkResults
            .filter(result => result.status === 'fulfilled' && result.value)
            .map(result => result.value);
        
        allResults.push(...successfulResults);
        
        console.log(`   ✅ Chunk ${chunkNumber}: ${successfulResults.length}/${chunkIds.length} sucessos`);
        
        // Pequeno delay entre chunks
        if (i + chunkSize < personIds.length) {
            await delay(CONFIG.DELAY_BETWEEN_BATCHES);
        }
    }
    
    return allResults;
}

// Função para buscar uma pessoa com delay inicial (evita rate limiting)
async function fetchSinglePersonWithDelay(personId, config, initialDelay = 0) {
    if (initialDelay > 0) {
        await delay(initialDelay);
    }
    
    const cacheKey = `person_${personId}`;
    
    // Verifica cache primeiro
    if (cache.people.has(cacheKey)) {
        return cache.people.get(cacheKey);
    }
    
    try {
        const response = await axios.get(
            `${BASE_URL}/people/${personId}`,
            config
        );

        const peopleData = response.data.results;
        
        if (!peopleData) {
            return null;
        }

        // Processar extrafields
        const extrafields = JSON.parse(peopleData.extrafields || '[]');
        const nomePai = extrafields.find(f => f.id_ef === 15819)?.value?.trim() || '';
        const nomeMae = extrafields.find(f => f.id_ef === 15820)?.value?.trim() || '';
        const naturalidade = extrafields.find(f => f.id_ef === 15823)?.value?.trim() || '';
        const funcao = determinarFuncao(extrafields);

        const processedData = {
            ...peopleData,
            nomePai,
            nomeMae,
            naturalidade,
            funcao,
            extrafieldsParsed: extrafields
        };

        // Salva no cache
        cache.people.set(cacheKey, processedData);
        
        return processedData;

    } catch (error) {
        // Silencia erro individual, mas loga se for frequente
        if (!error.message.includes('timeout')) {
            console.error(`      ❌ Erro pessoa ${personId}:`, error.message);
        }
        return null;
    }
}

// 5. Gerar dados do Excel
async function generateExcelData(groups, peopleMap, config) {
    const rows = [];
    
    // Cabeçalho
    rows.push([
        'Congregacao', 'Data Cadastro', 'Nome', 'Pai', 'Mãe', 'CPF',
        'Nascimento', 'Naturalidade', 'Função', 'Batismo', 'Rua',
        'Número', 'Bairro', 'CEP', 'Contato', 'Email', 'Estado Civil'
    ]);

    // Processar grupos em paralelo para montar as linhas
    const groupProcessingPromises = groups.map(async (group, index) => {
        try {
            const response = await axios.get(
                `${BASE_URL}/groups/${group.id}`,
                config
            );

            const membrosData = response.data.results;
            if (membrosData && membrosData.peoples) {
                const peopleIds = JSON.parse(membrosData.peoples || '[]');
                const groupRows = [];

                if (peopleIds.length > 0) {
                    // Processar cada pessoa deste grupo
                    peopleIds.forEach(personId => {
                        const person = peopleMap.get(personId);
                        if (person) {
                            const row = createPersonRow(person, group.name);
                            groupRows.push(row);
                        }
                    });

                    // Ordenar por nome
                    groupRows.sort((a, b) => a[2].localeCompare(b[2]));
                    
                    return {
                        groupIndex: index,
                        groupName: group.name,
                        rows: groupRows,
                        hasData: true
                    };
                }
            }
        } catch (error) {
            console.warn(`   ⚠️  Erro ao processar grupo ${group.name}: ${error.message}`);
        }
        
        return {
            groupIndex: index,
            groupName: group.name,
            rows: [],
            hasData: false
        };
    });

    // Executar todos os processamentos em paralelo
    const results = await Promise.allSettled(groupProcessingPromises);
    
    // Ordenar resultados pelo índice original
    const successfulResults = results
        .filter(result => result.status === 'fulfilled')
        .map(result => result.value)
        .sort((a, b) => a.groupIndex - b.groupIndex);
    
    // Adicionar linhas ao Excel
    successfulResults.forEach((result, index) => {
        if (result.hasData && result.rows.length > 0) {
            // Linha em branco entre grupos (exceto primeiro)
            if (index > 0) {
                rows.push(Array(17).fill(''));
            }
            
            rows.push(...result.rows);
        }
    });
    
    return rows;
}

// Função para criar linha da pessoa
function createPersonRow(person, groupName) {
    const extrafields = person.extrafieldsParsed || [];
    const nomePai = extrafields.find(f => f.id_ef === 15819)?.value?.trim() || '';
    const nomeMae = extrafields.find(f => f.id_ef === 15820)?.value?.trim() || '';
    const naturalidade = extrafields.find(f => f.id_ef === 15823)?.value?.trim() || '';
    const funcao = determinarFuncao(extrafields);
    
    let cpf = person.doc_1 || '';
    if (cpf && cpf.length === 11) {
        cpf = cpf.replace(/^(\d{3})(\d{3})(\d{3})(\d{2})$/, '$1.$2.$3-$4');
    }
    
    return [
        (groupName || '').toUpperCase(),
        formatarData(person.created_at),
        (person.full_name || '').toUpperCase(),
        nomePai.toUpperCase(),
        nomeMae.toUpperCase(),
        cpf,
        formatarData(person.birthydate),
        naturalidade.toUpperCase(),
        funcao,
        formatarData(person.baptism_date),
        (person.address_1 || '').toUpperCase(),
        person.address_number || '',
        (person.address_2 || '').toUpperCase(),
        person.postal_code || '',
        person.phone_1 || '',
        person.email || '',
        person.marital_status || ''
    ];
}

// 6. Gerar arquivo Excel
async function generateExcelFile(rows) {
    try {
        const ws = xlsx.utils.aoa_to_sheet(rows);
        
        const colWidths = [25, 20, 35, 35, 35, 15, 15, 25, 20, 15, 40, 10, 30, 12, 15, 25, 15];
        ws['!cols'] = colWidths.map(width => ({ wch: width }));
        
        ws['!autofilter'] = { ref: `A1:${xlsx.utils.encode_col(colWidths.length - 1)}1` };
        ws['!views'] = [{ state: 'frozen', xSplit: 0, ySplit: 1 }];
        
        const wb = xlsx.utils.book_new();
        xlsx.utils.book_append_sheet(wb, ws, 'Membros');
        
        const filename = `DADOS_ENUVENS_ULTRA_RAPIDO_${new Date().toISOString().slice(0, 10)}.xlsx`;
        xlsx.writeFile(wb, filename);
        
        console.log(`💾 Arquivo salvo como: ${filename}`);
        
        // Verificar tamanho
        const fs = require('fs');
        if (fs.existsSync(filename)) {
            const stats = fs.statSync(filename);
            console.log(`📏 Tamanho: ${(stats.size / 1024 / 1024).toFixed(2)} MB`);
        }
        
    } catch (error) {
        throw new Error(`Falha ao gerar Excel: ${error.message}`);
    }
}

// Funções auxiliares (mantidas iguais)
function formatarData(data) {
    if (!data) return "";
    try {
        if (data.length > 10) {
            const [ano, mes, resto] = data.split('-');
            const [dia, hora] = resto.split(' ');
            return `${dia}/${mes}/${ano} ${hora || ''}`.trim();
        } else {
            const [ano, mes, dia] = data.split("-");
            return `${dia}/${mes}/${ano}`;
        }
    } catch {
        return data;
    }
}

function determinarFuncao(extrafields) {
    const funcoes = ['MEMBRO', 'COOPERADOR', 'DIÁCONO', 'PRESBÍTERO', 'EVANGELISTA', 'PASTOR', 'CONGREGADO'];
    const campoFuncoes = extrafields.find(item => item.id_ef === "15822");
    
    if (!campoFuncoes || !campoFuncoes.sub) return 'NÃO INFORMADO';
    
    const indexAtivo = campoFuncoes.sub.findIndex(sub => sub.value === true);
    return indexAtivo !== -1 ? funcoes[indexAtivo] : 'NÃO INFORMADO';
}

// Executar
if (require.main === module) {
    fetchData().catch(console.error);
}

module.exports = { fetchData, formatarData, determinarFuncao };