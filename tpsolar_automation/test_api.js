const axios = require('axios');

const postURL = 'https://intelliqs.zometric.com/ax/tpsolar/api/xbarpost/';
const token = "55b4bffb2fd3d55ba632f4e8c974ff9440ff989f";

const payload = [
  {
    tag_id_default: "Diffusion1",
    time_stamp: "2025-11-04T23:20:00",
    individual_data_array: [196.8, 198.4, 181.6, 180.5, 178.6],
    mapchoicekey: [
      {
        spec: 122,
        choice_key: "Zone",
        value: "CGZ"
      },
      {
        spec: 122,
        choice_key: "Position",
        value: "Left"
      }
    ]
  }
];

(async () => {
  try {
    console.log("🚀 Sending test payload...");
    const response = await axios.post(postURL, payload, {
      headers: {
        Authorization: `Token ${token}`,
        'Content-Type': 'application/json'
      }
    });
    console.log("✅ API Response:", response.data);
  } catch (error) {
    console.error("❌ Error posting test payload:");
    console.error(error.response?.data || error.message);
  }
})();
