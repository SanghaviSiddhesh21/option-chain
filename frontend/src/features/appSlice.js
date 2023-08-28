import { createSlice } from "@reduxjs/toolkit";

const initialState = {
    symbolAndExpiry: '',
    symbolAndStrikeSprice: ''
};
const appSlice = createSlice({ name: 'app', initialState, reducers: { setSymbolAndExpiry: (state, action) => { state.symbolAndExpiry = action.payload }, setSymbolAndStrikeSprice: (state, action) => state.symbolAndStrikeSprice = action.payload } });

export const { setSymbolAndExpiry, setSymbolAndStrikeSprice } = appSlice.actions

export default appSlice.reducer