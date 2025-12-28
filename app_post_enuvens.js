const axios = require('axios');
require('dotenv').config();

const KEY_BEARER = process.env.KEY_BEARER;
const BASE_URL = process.env.BASE_URL;

async function criarPessoaEnuves() {

  const axiosHeaders = {
    Authorization: KEY_BEARER,
    Accept: 'application/json, text/plain, */*',
    'Content-Type': 'application/json',
    Referer: 'https://app.enuves.com/',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
  };

  const dados = {
    avatar_file: "",
    first_name: "MINA NATYELLE",
    last_name: "MORAES CORREA",
    password: "",
    employments: [257290],
    groups: [45962],
    categories: [],
    gender: "F",
    phone_1: "6200025588",
    phone_2: "",
    email: "",
    address_1: "RUA NITEROI",
    address_2: "PARQUE AMAZONAS",
    address_number: "",
    postal_code: "74843-140",
    id_city: 71917,
    id_state: "Goias",
    id_country: "BR",
    birthydate: "1995-06-11",
    doc_1: "025.333.000-88",
    doc_2: "",
    marital_status: "single",
    scholarity: "5",
    spouse_name: "",
    spouse_christian: "",
    conversion_date: "",
    baptism_status: 1,
    baptism_date: "2018-12-09",
    notes: "",
    extrafields: {
      "15819": "PEDRO EMANOEL CORREA",
      "15820": "MARIA ECLESIA MORAES CORREA",
      "15823": "GIPA - PA",
      "15825": "",
      "15826": false,
      "15821_0": true,
      "15821_1": false,
      "17295_0": true,
      "17295_1": false,
      "17295_2": false,
      "17295_3": false,
      "15822_0": true,
      "15822_1": false,
      "15822_2": false,
      "15822_3": false,
      "15822_4": false,
      "15822_5": false,
      "15822_6": false
    }
  };

  try {
    const response = await axios({
      method: 'post',
      url: `${BASE_URL}/people`,
      headers: axiosHeaders,
      data: dados,
      timeout: 30000
    });

    console.log('✅ Pessoa criada com sucesso!');
    console.log('Status:', response.status);
    console.log('Resposta:', response.data);

    return response.data;

  } catch (error) {
    console.error('❌ Erro ao criar pessoa:');

    if (error.response) {
      // Erro da API
      console.log('Status:', error.response.status);
      console.log('Data:', error.response.data);
      console.log('Headers:', error.response.headers);
    } else if (error.request) {
      // Erro de rede
      console.log('Erro de rede:', error.message);
    } else {
      // Erro na configuração
      console.log('Erro:', error.message);
    }

    throw error;
  }
}

// Chamar a função
setTimeout(() => {
  criarPessoaEnuves();
}, 5000);