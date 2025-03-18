import numpy as np
import os
from scipy.ndimage import gaussian_filter1d
import torch
import torch.nn as nn
import torch.nn.functional as F


############################################################
class LSTM_NA_Forecast_Model(nn.Module):
    def __init__(self):
        super(LSTM_NA_Forecast_Model, self).__init__()

        self.lstm_layer = nn.LSTM(input_size=2, hidden_size=256, num_layers=1, batch_first=True)

        self.dense_input = nn.Linear(256, 512)
        self.dense_layers = nn.ModuleList([nn.Linear(512, 256), nn.Linear(256, 128)])
        self.dense_output = nn.Linear(128, 8)
    
    def forward(self, input_data):
        # Get output of LSTM layer
        lstm_output, _ = self.lstm_layer(input_data)

        # Concatenate LSTM output with covariate data
        x = lstm_output[-1, :]
        x = self.dense_input(x)
        x = F.relu(x)
        for layer in self.dense_layers:
            x = layer(x)
            x = F.relu(x)
        x = self.dense_output(x)

        return x




############################################################
class LSTM_DEL_Forecast_Model(nn.Module):
    def __init__(self):
        super(LSTM_DEL_Forecast_Model, self).__init__()

        self.lstm_layer = nn.LSTM(input_size=2, hidden_size=256, num_layers=1, batch_first=True)

        self.dense_input = nn.Linear(262, 512)
        self.dense_layers = nn.ModuleList([nn.Linear(512, 256), nn.Linear(256, 128)])
        self.dense_output = nn.Linear(128, 8)
    
    def forward(self, input_data):
        # Unpack input data. ts_data is a list of tensors, cv_data is a tensor
        ts_data, cv_data = input_data

        # Get output of LSTM layer
        lstm_output, _ = self.lstm_layer(ts_data)

        # Concatenate LSTM output with covariate data
        x = torch.cat([lstm_output[-1, :], cv_data])
        x = self.dense_input(x)
        x = F.relu(x)
        for layer in self.dense_layers:
            x = layer(x)
            x = F.relu(x)
        x = self.dense_output(x)

        return x


############################################################
class GetForecast():
    def match_model(existing_rba):
        model_path = os.getcwd() + "/costar/src/forecast_models/"
        if existing_rba < 10000000:
            return None, None
        elif existing_rba < 20000000:
            na_model_params = torch.load(os.path.join(model_path,"10M-20M NA.pt"),map_location="cpu")
            del_model_params = torch.load(os.path.join(model_path,"10M-20M DEL.pt"),map_location="cpu")
        elif existing_rba < 45000000:
            na_model_params = torch.load(os.path.join(model_path,"20M-45M NA.pt"),map_location="cpu")
            del_model_params = torch.load(os.path.join(model_path,"20M-45M DEL.pt"),map_location="cpu")
        else:
            na_model_params = torch.load(os.path.join(model_path,"45M+ NA.pt"),map_location="cpu")
            del_model_params = torch.load(os.path.join(model_path,"45M+ DEL.pt"),map_location="cpu")
        
        return na_model_params, del_model_params


    def data_preprocessing(total_rba, hist_df, uc_sf):
        scaled_na = hist_df['NA'].values[::-1] * 1000 / total_rba
        scaled_del = hist_df['DEL'].values[::-1] * 1000 / total_rba
        scaled_uc_sf = np.array(uc_sf) * 1000 / total_rba

        # Smooth the data
        smooth_scaled_na = gaussian_filter1d(scaled_na, sigma=1.5, mode='nearest')
        smooth_scaled_del = gaussian_filter1d(scaled_del, sigma=1.5, mode='nearest')

        # Create array with first column being smoothed data, second column being original data
        na_data = torch.tensor(np.array([smooth_scaled_na, scaled_na]).T, dtype=torch.float32)
        del_data = torch.tensor(np.array([smooth_scaled_del, scaled_del]).T, dtype=torch.float32)
        uc_sf_data = torch.tensor(scaled_uc_sf, dtype=torch.float32)

        return na_data, [del_data, uc_sf_data]
    

    def forecast(na_model_input, del_model_input, na_model_params, del_model_params):
        na_model = LSTM_NA_Forecast_Model()
        na_model.load_state_dict(na_model_params)

        del_model = LSTM_DEL_Forecast_Model()
        del_model.load_state_dict(del_model_params)
        with torch.no_grad():
            na_forecast = na_model(na_model_input)
            del_forecast = del_model(del_model_input)
        return na_forecast, del_forecast
